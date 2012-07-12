HERE = $(shell pwd)
BIN = $(HERE)/bin
PYTHON = $(BIN)/python

PIP_DOWNLOAD_CACHE ?= $(HERE)/.pip_cache
INSTALL = $(BIN)/pip install
INSTALL += --download-cache $(PIP_DOWNLOAD_CACHE) -U --use-mirrors

BUILD_DIRS = bin build include lib lib64 man share


.PHONY: all build test

all: build

$(PYTHON):
	virtualenv --distribute .

build: $(PYTHON)
	$(INSTALL) http://gevent.googlecode.com/files/gevent-1.0b2.tar.gz
	$(INSTALL) zc-zookeeper-static
	$(PYTHON) setup.py develop
	$(INSTALL) kazoo[test]

clean:
	rm -rf $(BUILD_DIRS)

test:
	$(BIN)/nosetests -d --with-coverage kazoo
