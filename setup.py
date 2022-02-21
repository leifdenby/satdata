from setuptools import find_packages, setup

setup(
    name="satdata",
    packages=find_packages(exclude=["contrib", "tests", "docs"]),
    version="0.2.1",
    description="Satellite data access from Amazon S3 via python",
    author="Leif Denby",
    author_email="leifdenby@gmail.com",
    url="https://github.com/leifdenby/satdata",
    classifiers=[],
    install_requires=["tqdm", "s3fs"],
)
