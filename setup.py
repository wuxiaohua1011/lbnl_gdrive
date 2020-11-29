import datetime
from pathlib import Path

from setuptools import find_namespace_packages, setup, find_packages

setup(
    name="g_drive",
    author="The Materials Project",
    author_email="feedback@materialsproject.org",
    packages=find_packages(),
    license="modified BSD",
    zip_safe=False
)