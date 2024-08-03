from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="asp_sdk",
    version="0.1.0",
    author="Armando Aguilar L.",
    author_email="a.aguilar@gategeek.com",
    description="Convert Revit file formats and extract 3D model metadata",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ArmandAguilar/Revit-Model-Derivative-SDK",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "requests",
        "python-dotenv"
    ],
    package_data={
        "asp_sdk": ["images/*"],
    },
)