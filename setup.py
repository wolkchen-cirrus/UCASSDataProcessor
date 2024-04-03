from setuptools import setup, find_packages

setup(
    install_requires=[
        'pint',
        'pandas',
        'numpy',
        'scipy',
        'matplotlib',
        'pymavlink',
        'click'
    ],
    entry_points={
        'console_scripts': [
            'oproc = oproc.main:cli',
        ],
    },
)

