from setuptools import setup, find_packages
import re
import os

BASEDIR = os.path.dirname(os.path.abspath(__file__))

VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')

def get_version():
    init = open(os.path.join(BASEDIR, 'MODULE_NAME', '__init__.py')).read()
    return VERSION_RE.search(init).group(1)

# Dependencies (format is 'PYPI_PACKAGE_NAME[>]=VERSION_NUMBER')
BASE_DEPENDENCIES = [
]

# TEST_DEPENDENCIES = [
# ]

# DEVELOPMENT_DEPENDENCIES = [
# ]

# LOCAL_DEPENDENCIES = [
# ]

# Allow setup.py to be run from any path
os.chdir(os.path.normpath(BASEDIR))

setup(
    name='PYPI_PACKAGE_NAME',
    packages=find_packages(),
    version=get_version(),
    include_package_data=True,
    description='SHORT_DESCRIPTION',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='GITHUB_URL',
    author='AUTHOR_FULL_NAME',
    author_email='AUTHOR_EMAIL_ADDRESS',
    install_requires=BASE_DEPENDENCIES,
    # tests_require=TEST_DEPENDENCIES,
    # extras_require = {
    #     'test': TEST_DEPENDENCIES,
    #     'development': DEVELOPMENT_DEPENDENCIES,
    #     'local': LOCAL_DEPENDENCIES
    # },
    # entry_points={
    #     "console_scripts": [
    #          "COMMAND_NAME = MODULE_PATH:METHOD_NAME"
    #     ]
    # },
    keywords=[],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ]
)
