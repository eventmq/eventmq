"""
EventMQ setup.py file for distribution

"""

from setuptools import find_packages, setup

setup(
    name='eventmq',
    version='0.3.4.9',
    description='EventMQ job execution and messaging system based on ZeroMQ',
    packages=find_packages(),
    install_requires=['pyzmq==15.4.0',
                      'six==1.10.0',
                      'monotonic==0.4',
                      'croniter==0.3.10',
                      'redis==2.10.3',
                      'future==0.15.2',
                      'psutil==5.0.0',
                      'python-dateutil>=2.1,<3.0.0'],
    extras_require={
          'docs': ['Sphinx==1.5.2', ],
          'testing': [
              'flake8==3.2.1',
              'flake8-import-order==0.11',
              'flake8-print==2.0.2',
              'nose',
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

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',  # noqa
        'Operating System :: OS Independent',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    scripts=[
        'bin/emq-cli',
        'bin/emq-jobmanager',
        'bin/emq-router',
        'bin/emq-scheduler',
        'bin/emq-pubsub'
    ],
)
