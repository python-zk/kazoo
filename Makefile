HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

PIP_DOWNLOAD_CACHE ?= $(HERE)/.pip_cache
INSTALL = $(BIN)/pip install
INSTALL += --download-cache $(PIP_DOWNLOAD_CACHE) --use-mirrors

BUILD_DIRS = bin build include lib lib64 man share

GEVENT_VERSION ?= 1.0b4
PYTHON_EXE = $(shell [ -f $(PYTHON) ] && echo $(PYTHON) || echo python)
PYPY = $(shell $(PYTHON_EXE) -c "import sys; print(getattr(sys, 'pypy_version_info', False) and 'yes' or 'no')")
TRAVIS ?= false
TRAVIS_PYTHON_VERSION ?= $(shell $(PYTHON_EXE) -c "import sys; print('.'.join([str(s) for s in sys.version_info][:2]))")

ZOOKEEPER = $(BIN)/zookeeper
ZOOKEEPER_VERSION ?= 3.4.4
ZOOKEEPER_PATH ?= $(ZOOKEEPER)

GEVENT_SUPPORTED = yes
ifeq ($(findstring 3.,$(TRAVIS_PYTHON_VERSION)), 3.)
	GEVENT_SUPPORTED = no
endif
ifeq ($(PYPY),yes)
	GEVENT_SUPPORTED = no
endif

.PHONY: all build clean test zookeeper clean-zookeeper

all: build

$(PYTHON):
	python sw/virtualenv.py --distribute .
	rm distribute-0.6.*.tar.gz

build: $(PYTHON)
ifeq ($(GEVENT_SUPPORTED),yes)
	$(INSTALL) -U -r requirements_gevent.txt
	$(INSTALL) -f https://code.google.com/p/gevent/downloads/list?can=1 gevent==$(GEVENT_VERSION)
endif
ifneq ($(TRAVIS), true)
	$(INSTALL) -U -r requirements_sphinx.txt
endif
	$(INSTALL) -U -r requirements.txt
	$(PYTHON) setup.py develop
	$(INSTALL) kazoo[test]

clean:
	rm -rf $(BUILD_DIRS)

test:
	ZOOKEEPER_PATH=$(ZOOKEEPER_PATH) \
	$(BIN)/nosetests -d --with-coverage kazoo.tests -v

html:
	cd docs && \
	make html

$(ZOOKEEPER):
	@echo "Installing Zookeeper"
	mkdir -p bin
	cd bin && \
	curl --progress-bar http://apache.osuosl.org/zookeeper/zookeeper-$(ZOOKEEPER_VERSION)/zookeeper-$(ZOOKEEPER_VERSION).tar.gz | tar -zx
	mv bin/zookeeper-$(ZOOKEEPER_VERSION) bin/zookeeper
	cd bin/zookeeper
	chmod a+x bin/zookeeper/bin/zkServer.sh
	@echo "Finished installing Zookeeper"

zookeeper: $(ZOOKEEPER)

clean-zookeeper:
	rm -rf zookeeper bin/zookeeper
