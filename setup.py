"""
Gofka Python Client Setup
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setup(
    name="gofka-python",
    version="0.2.0",
    author="Gofka Contributors",
    author_email="",
    description="High-performance async Python client for Gofka message broker with compression, SSL/TLS, and connection pooling",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/user/gofka",
    project_urls={
        "Homepage": "https://github.com/user/gofka",
        "Documentation": "https://github.com/user/gofka/tree/main/docs",
        "Repository": "https://github.com/user/gofka",
        "Issues": "https://github.com/user/gofka/issues",
        "Changelog": "https://github.com/user/gofka/blob/main/CHANGELOG.md",
    },
    packages=find_packages(exclude=["tests", "tests.*", "examples", "examples.*", "benchmarks", "benchmarks.*"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Framework :: AsyncIO",
    ],
    keywords=[
        "gofka", "message-broker", "kafka", "client", "messaging",
        "distributed-systems", "async", "asyncio", "producer", "consumer",
        "streaming", "compression", "ssl", "tls"
    ],
    python_requires=">=3.7",
    install_requires=[],
    extras_require={
        "compression": [
            "python-snappy>=0.6.0",
            "lz4>=3.1.0",
        ],
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=3.0",
            "pytest-asyncio>=0.20",
            "black>=22.0",
            "pylint>=2.0",
            "mypy>=0.990",
            "ruff>=0.1.0",
            "python-snappy>=0.6.0",
            "lz4>=3.1.0",
        ],
        "all": [
            "python-snappy>=0.6.0",
            "lz4>=3.1.0",
        ],
    },
    license="MIT",
    include_package_data=True,
    zip_safe=False,
)
