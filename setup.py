
from setuptools import setup, find_packages


setup(
    name='foxy_sync',
    version="0.1",
    description="",
    author='Terrence Hu',
    author_email='huxt2013@163.com',
    url="https://github.com/huxt2014/foxy-sync",
    packages=find_packages(exclude=("test",)),
    scripts=["foxy-sync"],
    install_requires=["oss2==2.3.1"],
)