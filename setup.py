from setuptools import setup, find_packages
from sys import version_info

setup(
    name='tasr',
    version="0.1",
    description="Tagged Avro Schema Repository",
    package_dir = {'': 'src/py'},
    packages=find_packages(),
    include_package_data=True,
    install_requires=['avro>=1.7.5', 'redis'],
    
    # metadata for upload to PyPI
    author = 'Chris Mills',
    author_email = 'cmills@tagged.com',
    license = '',
    keywords = '',
    url = '',
)