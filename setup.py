import setuptools

setuptools.setup(
    name="data_project",
    packages=setuptools.find_packages(exclude=["etl_tests"]),
    install_requires=[
        "dagster==1.10.18",
        "dagit==0.15.0",
        "pytest",
    ],
)