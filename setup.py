import os
from pip.req import parse_requirements
from setuptools import setup, find_packages

# Reading the requirements from pip's requirements.txt
reqs = (str(ir.req) for ir in parse_requirements('requirements.txt'))

setup(
    name='airflow',
    version='0.11',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    scripts=['airflow/bin/airflow'],
    install_requires=reqs,
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/mistercrunch/Airflow',
    download_url='https://github.com/mistercrunch/Airflow/tarball/0.11',
)
