"""
EventMQ setup.py file for distribution

"""

from setuptools import setup, find_packages

setup(
    name='eventmq',
    version='0.3-rc4',
    description='EventMQ messaging system based on ZeroMQ',
    packages=find_packages(),
    install_requires=['pyzmq==15.4.0',
                      'six==1.10.0',
                      'monotonic==0.4',
                      'croniter==0.3.10',
                      'redis==2.10.3',
                      'future==0.15.2',
                      'psutil==5.0.0',
                      'python-dateutil>=2.1,<3.0.0'],
    author='EventMQ Contributors',
    url='https://github.com/enderlabs/eventmq/',

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
        'License :: OSI Approved :: GNU Lesser General Public License v2 (LGPLv2)',
        'Operating System :: OS Independent',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    scripts=[
        'bin/emq-cli',
        'bin/emq-jobmanager',
        'bin/emq-router',
        'bin/emq-scheduler',
        'bin/emq-pubsub'
    ],
)
