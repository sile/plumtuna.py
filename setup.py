from setuptools import find_packages
from setuptools import setup

setup(
    name="plumtuna",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["optuna", "requests"],
)
