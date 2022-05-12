from setuptools import setup, find_packages
import versioneer

with open("README.md") as file:
    long_description = file.read()

install_requirements = [
    "pip>=20.3",
    "psycopg[binary]>=3.0.13",
    "python-telegram-bot>=20.0a0",
    "tenacity>=8.0.1",
]

setup(
    name="telegres",
    version=versioneer.get_version(),
    author="Nischay Mamidi",
    author_email="NischayPro@protonmail.com",
    description="A Python library to add PostgresPersistence support to Telegram Python Bot.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Nischay-Pro/telegres",
    project_urls={
        "Bug Tracker": "https://github.com/Nischay-Pro/telegres/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Operating System :: POSIX :: Linux",
    ],
    cmdclass=versioneer.get_cmdclass(),
    install_requires=install_requirements,
    packages=find_packages(),
    python_requires=">=3.7",
    zip_safe=True,
)
