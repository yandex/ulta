from setuptools import setup, find_namespace_packages
from ulta.version import VERSION


setup(
    package_dir={'': '.'},
    name='ulta',
    version=VERSION,
    description='''
ULTA is a performance measurement and load testing automatization tool.
It uses other load generators such as JMeter, ab or phantom inside of it for
load generation and provides a common configuration system for them and
analytic tools for the results they produce.
''',
    python_requires='>=3.10',
    maintainer='Yandex Load Team',
    maintainer_email='load@yandex-team.ru',
    url='http://yandex.github.io/ulta/',
    packages=find_namespace_packages(include=['ulta', 'ulta.*']),
    install_requires=[
        'requests>=2.5.1',
        'pyyaml>=5.4',
        'grpcio',
        'grpcio-tools',
        'PyJWT',
        'yandextank>=2.0.0',
        'yandexcloud>=0.216.0',
        'protobuf',
        'google-api-core>=2.11.0',
        'pydantic>=2.0.0',
        'grpcio-status',
        'strenum',
    ],
    tests_require=['pytest==4.6.3', 'flake8', 'pytest-benchmark', 'zipp==0.5.1', 'mock'],
    license='Apache',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: Apache License 2.0',
        'Operating System :: POSIX',
        'Topic :: Software Development :: Quality Assurance',
        'Topic :: Software Development :: Testing',
        'Topic :: Software Development :: Testing :: Traffic Generation',
        'Programming Language :: Python :: 3.10',
    ],
    entry_points={
        'console_scripts': [
            'ulta = ulta.cli:main',
        ],
    },
)
