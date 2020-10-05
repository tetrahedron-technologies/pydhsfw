from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pydhsfw',
    version='0.1.0',
    description='Distributed Hardware Server Framework written in Python',
    long_description=readme,
    author='Giles Mullen',
    author_email='gcmullen@sbcglobal.net',
    #url='https://github.com/gcmullen/pydhsfw',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=['PyYAML', 'dotty-dict']
)