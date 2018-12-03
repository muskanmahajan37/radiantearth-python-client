#import setuptools
from distutils.core import setup

setup(
    name="radiantearth",
    version="0.5.0",
    description='A Python client for Radiant Earth Foundation platform, a web tool for '
    'combining, analyzing, and publishing raster data.',
    long_description=open('README.rst').read(),
    url='https://github.com/radiantearth/radiantearth-python-client',
    author='Radiant Earth Foundation',
    author_email='code@radiant.earth',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
    keywords='raster earth-observation geospatial geospatial-processing \
            radiant earth global-development',
    packages=setup.find_packages(exclude=['tests']),
    package_data={'': ['*.yml']},
    install_requires=[
        'cryptography >= 2.0.0',
        'pyasn1 >= 0.2.3',
        'requests >= 2.9.1',
        'bravado >= 8.4.0',
        'boto3 >= 1.4.4',
        'future >= 0.16.0',
        'shapely >= 1.6.4post1',
        'Cython' = 0.29,
        'Cartopy' = 0.16.0,
        
    ],
    extras_require={
        'notebook': [
            'notebook >= 4.0.0',
            'az-ipyleaflet==0.4.1'
        ],
        'dev': [],
        'test': [],
    },
    tests_require=[]
)
