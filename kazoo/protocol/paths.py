def normpath(path: str, trailing: bool = False) -> str:
    """Normalize path, eliminating double slashes, etc."""
    comps = path.split("/")
    new_comps = []
    for comp in comps:
        if comp == "":
            continue
        if comp in (".", ".."):
            raise ValueError("relative paths not allowed")
        new_comps.append(comp)
    new_path = "/".join(new_comps)
    if trailing is True and path.endswith("/"):
        new_path += "/"
    if path.startswith("/") and new_path != "/":
        return "/" + new_path
    return new_path


def join(a: str, *p: str) -> str:
    """Join two or more pathname components, inserting '/' as needed.

    If any component is an absolute path, all previous path components
    will be discarded.

    """
    path = a
    for b in p:
        if b.startswith("/"):
            path = b
        elif path == "" or path.endswith("/"):
            path += b
        else:
            path += "/" + b
    return path


def isabs(s: str) -> bool:
    """Test whether a path is absolute"""
    return s.startswith("/")


def basename(p: str) -> str:
    """Returns the final component of a pathname"""
    i = p.rfind("/") + 1
    return p[i:]


def _prefix_root(root: str, path: str, trailing: bool = False) -> str:
    """Prepend a root to a path."""
    return normpath(
        join(_norm_root(root), path.lstrip("/")), trailing=trailing
    )


def _norm_root(root: str) -> str:
    return normpath(join("/", root))
