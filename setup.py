#!/usr/bin/env python3
"""
Political Ad Collector - Setup Script

Install with: pip install -e .
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read requirements
requirements_path = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_path.exists():
    requirements = [
        line.strip()
        for line in requirements_path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]

setup(
    name="political-ad-collector",
    version="0.1.0",
    description="Modular framework for collecting political advertising data from multiple platforms",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/political-ad-collector",
    license="MIT",
    packages=find_packages(exclude=["tests", "tests.*", "output", "output.*"]),
    include_package_data=True,
    package_data={
        "collectors.meta": ["config.yaml", "README.md"],
        "config": ["*.yaml"],
    },
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "black>=23.12.0",
            "flake8>=6.1.0",
            "mypy>=1.7.1",
        ],
        "gcp": [
            "google-cloud-storage>=2.14.0",
            "google-cloud-secret-manager>=2.16.4",
            "google-cloud-bigquery>=3.14.0",
            "functions-framework>=3.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "collect-meta-ads=scripts.run_meta_collector:main",
            "test-ad-credentials=scripts.test_credentials:main",
            "upload-to-bigquery=scripts.upload_to_bigquery:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
    ],
    keywords="political ads, advertising, meta, facebook, google, transparency, data collection",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/political-ad-collector/issues",
        "Source": "https://github.com/yourusername/political-ad-collector",
    },
)
