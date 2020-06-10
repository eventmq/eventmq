import ast

from setuptools import find_packages, setup

version = 'unknown'
with open('eventmq/__init__.py') as f:
    for line in f:
        if line.startswith('__version__'):
            version = ast.parse(line).body[0].value.s
            break

setup(
    name='eventmq',
    version=version,
    description='EventMQ job execution and messaging system based on ZeroMQ',
    packages=find_packages(),
    install_requires=[
        'pyzmq==18.1.0',
        'six>=1.14,<2',
        'monotonic==0.4',
        'croniter==0.3.10',
        'future==0.15.2',
        'psutil==5.6.6',
    ],
    extras_require={
          'docs': ['Sphinx==1.5.2', ],
          'testing': [
              'flake8==3.7.8',
              'flake8-import-order==0.18.1',
              'flake8-print==3.1.0',
              'coverage==4.0.3',
              'testfixtures==4.7.0',
              'freezegun==0.3.7',
              'tl.testing==0.5',
              'mock==1.3.0'],
          },
    author='EventMQ Contributors',
    url='https://github.com/eventmq/eventmq/',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: System :: Distributed Computing',

        'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',  # noqa
        'Operating System :: OS Independent',

        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    scripts=[
        'bin/emq-cli',
        'bin/emq-jobmanager',
        'bin/emq-router',
        'bin/emq-scheduler',
        'bin/emq-pubsub'
    ],
)
