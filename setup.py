from setuptools import setup, find_packages

setup(
    name='satdata',
    packages=find_packages(exclude=['contrib', 'tests', 'docs']),
    version='0.1.3',
    description='Satellite data access from Amazon S3 via python',
    author='Leif Denby',
    author_email='leifdenby@gmail.com',
    url='https://github.com/leifdenby/satdata',
    classifiers=[],
    install_requires=["satpy", "xarray", "boto3", "tqdm",
                      "progressbar"]
)
