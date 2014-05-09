from setuptools import setup, find_packages
from sys import version_info

setup(
    name='tasr',
    version="0.2",
    description="Tagged Avro Schema Repository",
    package_dir = {'': 'src/py'},
    packages=find_packages(),
    include_package_data=True,
    install_requires=['avro','bottle','redis','requests'],
    
    # metadata for upload to PyPI
    author = 'Chris Mills',
    author_email = 'cmills@tagged.com',
    license = '',
    keywords = 'Redis Avro Schema Repository Camus',
    url = '',
)