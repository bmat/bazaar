from setuptools import setup

with open('requirements.txt') as fp:
    install_requires = fp.read()

setup(
    name='bazaar',
    packages=['bazaar'],
    version='0.9',
    description='Agnostic file storage',
    author='BMAT developers',
    author_email='tv-av@bmat.com',
    url='https://github.com/bmat/bazaar',
    download_url='https://github.com/bmat/bazaar/archive/master.zip',
    keywords=['storage', 's3', 'ftp', 'file', 'mongo', 'mongodb', 'fs', 'filesystem'],
    classifiers=['Topic :: Adaptive Technologies', 'Topic :: Software Development', 'Topic :: System',
                 'Topic :: Utilities'],
    install_requires=install_requires
)
