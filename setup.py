__version__ = '0.2'

import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.rst')) as f:
    CHANGES = f.read()

install_requires = [
    'zope.interface >= 3.8.0',  # has zope.interface.registry
]

tests_require = install_requires + [
    'Sphinx',
    'docutils',
    'repoze.sphinx.autointerface',
    'gevent',
    ]

on_rtd = os.environ.get('READTHEDOCS', None) == 'True'
if on_rtd:
    install_requires.extend([
        'gevent'
    ])

setup(
    name='kazoo',
    version=__version__,
    description='Higher Level Zookeeper Client',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Networking",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords='zookeeper lock leader configuration',
    author="Nimbus team, Zope Corporation, Mozilla Foundation",
    author_email="",
    url="http://kazoo.readthedocs.org/",
    license="Apache 2.0",
    packages=find_packages(),
    test_suite="kazoo.tests",
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={
    },
    entry_points="""
    """
)
