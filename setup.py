import setuptools
import oproc

with open("README.md", "r") as f:
    long_description = f.read()

packages = setuptools.find_packages()

setuptools.setup(
    name="oproc",
    version=oproc.__version__,
    author=oproc.__author__,
    author_email=oproc.__email__,
    description=oproc.__doc__,
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
