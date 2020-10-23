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
    url='https://github.com/tetrahedron-technologies/pydhsfw',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    python_requires='>=3.6',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    install_requires=[
        'PyYAML',
        'dotty-dict',
        'coloredlogs',
        'verboselogs',
        'requests'
    ]
)
