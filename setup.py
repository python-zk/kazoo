
import os
import sys
import re

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.md')) as f:
    CHANGES = f.read()
version = ''
with open(os.path.join(here, 'kazoo', 'version.py')) as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        f.read(), re.MULTILINE).group(1)

PYTHON3 = sys.version_info > (3, )
PYPY = getattr(sys, 'pypy_version_info', False) and True or False

install_requires = ['six']

tests_require = install_requires + [
    'coverage',
    'mock',
    'nose',
    'flake8',
]

if not (PYTHON3 or PYPY):
    tests_require += [
        'gevent',
        'eventlet',
    ]

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
if on_rtd:
    install_requires.extend([
        'gevent',
        'eventlet',
    ])

setup(
    name='kazoo',
    version=version,
    description='Higher Level Zookeeper Client',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Networking",
    ],
    keywords='zookeeper lock leader configuration',
    author="Kazoo team",
    author_email="python-zk@googlegroups.com",
    url="https://kazoo.readthedocs.io",
    license="Apache 2.0",
    packages=find_packages(),
    test_suite="kazoo.tests",
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
)
