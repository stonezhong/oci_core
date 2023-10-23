import os
from setuptools import setup

# The directory containing this file
HERE = os.path.dirname(os.path.abspath(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md"), "r") as f:
    README = f.read()

# This call to setup() does all the work
setup(
    name="oci-core",
    version="0.0.79",
    description="OCI Core Helper",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://bitbucket.oci.oraclecorp.com/projects/HWD/repos/hadoop/browse/etl/sharedLibs/oci_core",
    author="Stone Zhong",
    author_email="shidong.zhong@oracle.com",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    packages=["oci_core"],
    install_requires=["oci", "cachetools"],
)
