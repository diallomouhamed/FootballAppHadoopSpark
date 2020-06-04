from setuptools import setup, find_packages
setup(
    name="FootballApp",
    version="0.1",
    packages=find_packages(),

    install_requires=["docutils>=0.3"],
    package_data={
    "": ["*.txt", "*.rst", "*.csv"]
    },
    
    author="Mouhamed DIALLO",
    author_email="mouhameddiallo088@gmail.com",
    description="ESGI project",

    python_requires='>=3.7'
)