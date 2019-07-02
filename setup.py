# pylint: disable = missing-docstring
from setuptools import find_packages, setup

setup(name="common-crawler",
      version="0.1",
      packages=find_packages(),
      install_requires=['boto3', 'cytoolz', 'requests', 'urllib3'],
      description='Load htmls from Common Crawl')
