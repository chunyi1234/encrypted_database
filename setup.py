from setuptools import find_packages, setup

setup(
    name="encrypted_database",
    packages=find_packages("encrypted_database"),
    install_requires=[
        "cryptography >= 38.0.3",
        "pymongo >= 4.3.3",
        "rocksdict >= 0.3.4",
    ],
)
