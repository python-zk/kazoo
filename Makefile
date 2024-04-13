HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python
INSTALL = $(BIN)/pip install
TOX_VENV ?= py37
BUILD_DIRS = bin build include lib lib64 man share

PYTHON_EXE = $(shell [ -f $(PYTHON) ] && echo $(PYTHON) || echo python)
PYPY = $(shell $(PYTHON_EXE) -c "import sys; print(getattr(sys, 'pypy_version_info', False) and 'yes' or 'no')")
CI ?= false
CI_PYTHON_VERSION ?= $(shell $(PYTHON_EXE) -c "import sys; print('.'.join([str(s) for s in sys.version_info][:2]))")

GREENLET_SUPPORTED = yes
ifeq ($(findstring 3.,$(CI_PYTHON_VERSION)), 3.)
	GREENLET_SUPPORTED = no
	VENV_CMD = $(PYTHON_EXE) -m venv .
else
	VENV_CMD = $(PYTHON_EXE) -m virtualenv .
endif
ifeq ($(PYPY),yes)
	GREENLET_SUPPORTED = no
endif

.PHONY: all build clean test

all: build

$(PYTHON):
	$(VENV_CMD)

build: $(PYTHON)
ifeq ($(GREENLET_SUPPORTED),yes)
	$(INSTALL) -U -r requirements_eventlet.txt
	$(INSTALL) -U -r requirements_gevent.txt
endif
ifneq ($(CI), true)
	$(INSTALL) -U -r requirements_sphinx.txt
endif
	$(INSTALL) -U -r requirements.txt
	$(PYTHON) setup.py develop
	$(INSTALL) kazoo[test]

clean:
	rm -rf $(BUILD_DIRS)

test:
	tox -e$(TOX_VENV)

html:
	cd docs && \
	make html
