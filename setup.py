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
        'requests>=2.31.0',
        'pyyaml>=5.4',
        'grpcio>=1.64.0,<2',
        'grpcio-tools',
        'PyJWT',
        'yandextank>=2.0.0',
        'yandexcloud>=0.310.0',
        'protobuf',
        'google-api-core>=2.17.1',
        'pydantic>=2.5.3',
        'grpcio-status',
        'strenum',
        'tabulate',
        'tenacity',
        'boto3>=1.34.0',
        'cachetools>=5.3.0',
    ],
    tests_require=['pytest>=7.4.4', 'flake8', 'pytest-benchmark', 'zipp==0.5.1', 'mock'],
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
