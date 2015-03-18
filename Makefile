HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python
PIP_DOWNLOAD_CACHE ?= $(HERE)/.pip_cache
INSTALL = $(BIN)/pip install
INSTALL += --download-cache $(PIP_DOWNLOAD_CACHE)
TOX_VENV ?= py27
BUILD_DIRS = bin build include lib lib64 man share

GEVENT_VERSION ?= 1.0.1
PYTHON_EXE = $(shell [ -f $(PYTHON) ] && echo $(PYTHON) || echo python)
PYPY = $(shell $(PYTHON_EXE) -c "import sys; print(getattr(sys, 'pypy_version_info', False) and 'yes' or 'no')")
TRAVIS ?= false
TRAVIS_PYTHON_VERSION ?= $(shell $(PYTHON_EXE) -c "import sys; print('.'.join([str(s) for s in sys.version_info][:2]))")

GREENLET_SUPPORTED = yes
ifeq ($(findstring 3.,$(TRAVIS_PYTHON_VERSION)), 3.)
	GREENLET_SUPPORTED = no
endif
ifeq ($(PYPY),yes)
	GREENLET_SUPPORTED = no
endif

.PHONY: all build clean test

all: build

$(PYTHON):
	python sw/virtualenv.py .

build: $(PYTHON)
ifeq ($(GREENLET_SUPPORTED),yes)
	$(INSTALL) -U -r requirements_eventlet.txt
	$(INSTALL) -U -r requirements_gevent.txt
	$(INSTALL) -f https://github.com/surfly/gevent/releases gevent==$(GEVENT_VERSION)
endif
ifneq ($(TRAVIS), true)
	$(INSTALL) -U -r requirements_sphinx.txt
endif
	$(INSTALL) -U -r requirements.txt
	$(PYTHON) setup.py develop
	$(INSTALL) kazoo[test]

clean:
	rm -rf $(BUILD_DIRS)

test: $(PYTHON)
	tox -e$(TOX_VENV)

html:
	cd docs && \
	make html
