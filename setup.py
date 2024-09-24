from pathlib import Path

import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


def get_version():
    """Retrieve package version."""
    version_path = Path() / "taegis_magic" / "_version.py"
    if not version_path.exists():
        raise RuntimeError(f"{version_path.name} does not exist")

    for line in version_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("__version__"):
            quote = '"' if '"' in line else "'"
            version = line.split()[2]
            return version.replace(quote, "")
    raise RuntimeError("Unable to read version.")


setuptools.setup(
    name="taegis-magic",
    version=get_version(),
    author="Micah Pegman",
    author_email="sdks@secureworks.com",
    description="Taegis IPython Magics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/secureworks/taegis-magics",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "ipython",
        "nbformat",
        "typer[all]>=0.9.0",
        "pandas",
        "pandas[excel]",
        "taegis-sdk-python",
        "Jinja2",
        "compress_pickle>=2.1.0",
        "gql",
        "dataclasses_json",
        "click",
        "requests",
        "ipynbname",
        "panel",
        "tabulator",
    ],
    python_requires=">=3.8",
    entry_points={"console_scripts": ["taegis = taegis_magic.cli:cli"]},
)
