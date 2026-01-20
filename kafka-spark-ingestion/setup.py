from setuptools import setup, find_packages

setup(
    name="kafka_spark_ingestion",
    version="1.0.0",
    description="Reusable Spark pipeline component for ingesting data from Kafka Confluent topics",
    author="Data Engineering Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.5.0",
        "confluent-kafka>=2.3.0",
        "pyyaml>=6.0",
        "avro-python3>=1.10.0",
        "requests>=2.31.0",
    ],
    include_package_data=True,
    package_data={
        "": ["config/*.yml"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
)
