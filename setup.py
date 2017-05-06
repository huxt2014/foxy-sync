
from setuptools import setup, find_packages
import foxy_sync


setup(
    name='foxy_sync',
    version=foxy_sync.version,
    description="",
    author='Terrence Hu',
    author_email='huxt2013@163.com',
    url="https://github.com/huxt2014/foxy-sync",
    packages=find_packages(),
    scripts=["foxy-sync"],
    install_requires=["oss2==2.3.1"],
)