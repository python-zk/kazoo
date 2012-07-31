HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

PIP_DOWNLOAD_CACHE ?= $(HERE)/.pip_cache
INSTALL = $(BIN)/pip install
INSTALL += --download-cache $(PIP_DOWNLOAD_CACHE) -U --use-mirrors

BUILD_DIRS = bin build include lib lib64 man share

ZOOKEEPER = $(BIN)/zookeeper
ZOOKEEPER_VERSION = 3.3.5
ZOOKEEPER_PATH ?= $(ZOOKEEPER)

.PHONY: all build clean test zookeeper clean-zookeeper

all: build

$(PYTHON):
	virtualenv --distribute .

build: $(PYTHON)
	$(INSTALL) http://gevent.googlecode.com/files/gevent-1.0b3.tar.gz
	$(INSTALL) zc-zookeeper-static
	$(PYTHON) setup.py develop
	$(INSTALL) kazoo[test]

clean:
	rm -rf $(BUILD_DIRS)

test:
	export ZOOKEEPER_PATH=$(ZOOKEEPER_PATH) && \
	$(BIN)/nosetests -d -v --with-coverage kazoo

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
	cd bin/zookeeper/src/c && \
	./configure && \
	make
	cd bin/zookeeper/src/contrib/zkpython && \
	mv build.xml old_build.xml && \
	cat old_build.xml | sed 's|executable="python"|executable="../../../../../bin/python"|g' > build.xml && \
	ant install
	chmod a+x bin/zookeeper/bin/zkServer.sh
	@echo "Finished installing Zookeeper"

zookeeper: $(ZOOKEEPER)

clean-zookeeper:
	rm -rf zookeeper bin/zookeeper
