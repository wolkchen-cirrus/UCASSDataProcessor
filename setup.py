from setuptools import setup, find_packages

setup(
    install_requires=[
        'pint',
        'h5py',
        'pandas',
        'numpy',
        'scipy',
        'matplotlib',
        'pymavlink',
        'click',
        'tabulate',
        'cartopy'
    ],
    entry_points={
        'console_scripts': [
            'oproc = oproc.main:cli',
        ],
    },
)

