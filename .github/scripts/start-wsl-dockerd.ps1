# Idempotently ensure a Linux dockerd runs inside WSL2 and is reachable from
# the Windows docker CLI over TCP.
#
# Callable from any job step (PowerShell via `pwsh -File`, or bash with
# `pwsh -NoProfile -NonInteractive -File`). Safe to run repeatedly: if the
# daemon is already reachable it is a fast-path no-op; otherwise it writes the
# WSL idle settings, (re)installs the distro if missing and (re)launches
# dockerd, then finds an endpoint the *Windows* CLI can actually dial and
# publishes it (GITHUB_ENV + a file the test step sources).
#
# Background: the hosted Windows engine (Moby) only serves Windows containers,
# while the official ZooKeeper image is linux-only, so the ensemble suite runs
# against a Linux daemon in WSL2. The WSL2 127.0.0.1 localhost relay
# is unreliable on hosted runners (the Windows CLI cannot reach
# tcp://localhost:2375 even though dockerd is up), so we fall back to the WSL
# VM's NAT IP, which the Windows host can always route to via the WSL vSwitch.
# Bind-mount sources are translated to the daemon's /mnt/<drive> layout by
# kazoo.testing.common, so /mnt/d:/... is handled there, not here.

$ErrorActionPreference = 'Stop'
$Distro = 'Ubuntu'

function Test-WindowsDocker {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { return $false }
    $os = (docker info --format '{{.OSType}}' 2>$null | Out-String).Trim()
    return ($os -eq 'linux')
}

function Publish-Host {
    # Record the working endpoint for the test step (GITHUB_ENV reaches later
    # steps; the file is sourced in the same step for belt and suspenders).
    $file = Join-Path $env:GITHUB_WORKSPACE '.start-wsl-dockerd.host'
    Set-Content -Path $file -Value $env:DOCKER_HOST -NoNewline
    Add-Content -Path $env:GITHUB_ENV -Value "DOCKER_HOST=$env:DOCKER_HOST"
}

# Fast path: the host exported by the setup step (e.g. the WSL NAT IP) still
# answers, e.g. when the test step re-invokes this script.
if ($env:DOCKER_HOST -and (Test-WindowsDocker)) {
    Write-Host "dockerd already reachable at $env:DOCKER_HOST"
    Publish-Host
    exit 0
}

# Prefer the WSL2 127.0.0.1 relay where the host supports it.
$env:DOCKER_HOST = 'tcp://localhost:2375'
if (Test-WindowsDocker) {
    Write-Host "dockerd reachable via the WSL relay at $env:DOCKER_HOST"
    Publish-Host
    exit 0
}

# Keep the WSL VM resident across steps so the daemon does not die in the gap
# between job steps (default ~60s idle reap). Applied once; `wsl --shutdown`
# forces the next VM boot to pick the config up.
$wslConfig = Join-Path $env:USERPROFILE '.wslconfig'
if (-not (Test-Path $wslConfig) -or
    -not (Get-Content $wslConfig -Raw -ErrorAction SilentlyContinue |
          Select-String -Quiet 'vmIdleTimeout')) {
    Set-Content -Path $wslConfig -Encoding ascii -Value @'
[wsl2]
# Keep the VM (and the background dockerd) alive across job steps.
vmIdleTimeout=2147483647
'@
    wsl.exe --shutdown
    Write-Host "wrote $wslConfig (vmIdleTimeout), reset WSL"
}

# Make sure the distro exists (first call in a job performs the install).
# -u root keeps the probe consistent with how dockerd is started (no reliance
# on the distro's default user being configured yet).
wsl.exe -d $Distro -u root -e true 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "installing $Distro (first boot on this runner)"
    wsl.exe --install $Distro --no-launch
}

# (Re)start dockerd inside WSL. Piped via stdin rather than a command line so
# wsl.exe cannot mangle shell specials; CRLF is normalized to LF so checkout
# line endings never leak into bash. The trailing `sleep` keeps the VM (and
# thus dockerd) resident for the rest of the job even if the idle reaper would
# otherwise fire; dockerd itself detaches via nohup.
$dockerdSetup = @'
set -eux
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io
# Ubuntu may already run a systemd-managed dockerd on the unix socket; stop it
# so our manually-launched daemon -- which also listens on tcp://0.0.0.0:2375
# for the Windows client -- can bind.
service docker stop >/dev/null 2>&1 || true
pkill -x dockerd >/dev/null 2>&1 || true
nohup dockerd --host=unix:///var/run/docker.sock --host=tcp://0.0.0.0:2375 >/tmp/dockerd.log 2>&1 </dev/null &
nohup sleep 21600 >/dev/null 2>&1 </dev/null &
for i in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 2; done
# Trailing comment so any CRLF appended by the local pipe lands on a comment
# line instead of becoming a stray command.
# end
'@.Replace("`r`n", "`n").Replace("`r", "`n")

$dockerdSetup | wsl.exe -d $Distro -u root -- bash -s
if ($LASTEXITCODE -ne 0) {
    wsl.exe -d $Distro -u root -- cat /tmp/dockerd.log
    throw "WSL2 dockerd (re)start failed"
}

# Dial candidates in order: localhost relay (explicit 127.0.0.1, since some
# hosts resolve `localhost` to ::1 which the relay never binds), every IP the
# guest reports. The first one the Windows CLI can reach wins -- this is the
# exact path the test harness uses, so failing here is fatal.
$candidates = @('tcp://localhost:2375', 'tcp://127.0.0.1:2375')
$wslIpRaw = (wsl.exe -d $Distro -u root -- hostname -I 2>$null | Out-String).Trim()
foreach ($ip in ($wslIpRaw -split '\s+' | Where-Object { $_ })) {
    $candidates += "tcp://${ip}:2375"
}

$deadline = (Get-Date).AddSeconds(90)
$working = $null
while (-not $working) {
    foreach ($h in $candidates) {
        $env:DOCKER_HOST = $h
        if (Test-WindowsDocker) {
            $working = $h
            break
        }
    }
    if ($working) { break }
    if ((Get-Date) -gt $deadline) {
        wsl.exe -d $Distro -u root -- cat /tmp/dockerd.log
        throw "Windows docker CLI cannot reach the WSL dockerd (tried: $($candidates -join ', '))"
    }
    Start-Sleep -Seconds 2
}
Write-Host "dockerd ready at $env:DOCKER_HOST (OSType=linux)"
Publish-Host
exit 0