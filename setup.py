"""
EventMQ setup.py file for distribution

"""

from setuptools import setup, find_packages

setup(
    name='eventmq',
    version='0.1.9',
    description='EventMQ messaging system based off ZeroMQ',
    packages=find_packages(),
    install_requires=['pyzmq==14.6.0',
                      'six==1.10.0',
                      'monotonic==0.4',
                      'croniter==0.3.10',
                      'sphinxcontrib-napoleon==0.4.3',
                      'Sphinx==1.3.1',
                      'nose==1.3.6',
                      'coverage==4.0.3',
                      'testfixtures==4.7.0',
                      'future==0.15.2',
                      'redis==2.10.3',
                      'mock==1.3.0',
                      'tl.testing==0.5',
                      'python-dateutil>=2.1,<3.0.0'],


    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],

    entry_points={
        'console_scripts': [
            'emq-router = eventmq.router:router_main',
            'emq-jobmanager = eventmq.jobmanager:jobmanager_main',
            'emq-scheduler = eventmq.scheduler:scheduler_main'
        ]
    }
)
