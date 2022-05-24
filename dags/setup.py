import setuptools

setuptools.setup(
    name="etl_project",
    version="0.0.1",
    packages=setuptools.find_packages(),
    install_requires=["pyspark==3.2.1", "six"]
)
