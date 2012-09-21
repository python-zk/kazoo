HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

PIP_DOWNLOAD_CACHE ?= $(HERE)/.pip_cache
INSTALL = $(BIN)/pip install
INSTALL += --download-cache $(PIP_DOWNLOAD_CACHE) --use-mirrors

BUILD_DIRS = bin build include lib lib64 man share

GEVENT_VERSION ?= 1.0b4
PYTHON_EXE = $(shell [ -f $(PYTHON) ] && echo $(PYTHON) || echo python)
TRAVIS_PYTHON_VERSION ?= $(shell $(PYTHON_EXE) -c "import sys; print('.'.join([str(s) for s in sys.version_info][:2]))")

ZOOKEEPER = $(BIN)/zookeeper
ZOOKEEPER_VERSION ?= 3.3.6
ZOOKEEPER_PATH ?= $(ZOOKEEPER)

.PHONY: all build clean test zookeeper clean-zookeeper

all: build

$(PYTHON):
	python sw/virtualenv.py --distribute .
	rm distribute-0.6.*.tar.gz

build: $(PYTHON)
ifeq ($(TRAVIS_PYTHON_VERSION),3.2)
	$(INSTALL) -U -r requirements3.txt
else
	$(INSTALL) -U -r requirements.txt
	$(INSTALL) -f https://code.google.com/p/gevent/downloads/list?can=1 gevent==$(GEVENT_VERSION)
endif
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
	cd bin/zookeeper && ant compile
	chmod a+x bin/zookeeper/bin/zkServer.sh
	@echo "Finished installing Zookeeper"

zookeeper: $(ZOOKEEPER)

clean-zookeeper:
	rm -rf zookeeper bin/zookeeper
