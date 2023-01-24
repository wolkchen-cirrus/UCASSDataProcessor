import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

packages = setuptools.find_packages()

setuptools.setup(
    name="noether",
    version="0.0.1",
    author="Jessica Marie Girdwood",
    author_email="jessgirdwood@protonmail.com",
    description="Processes data from the UCASS instrument, and manages the data repos",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/JGirdwood/UCASSDataProcessor",
    packages=packages,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: MIT",
        "Topic :: Scientific/Engineering",
        "Development Status :: Development",
    ],
    keywords="climate atmosphere cloud physics measurement opc uav",
    python_requires='>=3.7',
)