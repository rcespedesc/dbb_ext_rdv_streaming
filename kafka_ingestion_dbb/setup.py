from setuptools import setup, find_packages

setup(
    name="kafka-ingestion-dbb",
    version="1.0.0",
    description="Data Building Block para ingesta de Kafka Confluent a Delta Lake",
    author="Your Team",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.3.0",
        "pyyaml>=6.0",
        "confluent-kafka[avro]>=2.0.0",
        "fastavro>=1.7.0",
        "jsonschema>=4.17.0",
        "requests>=2.28.0",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
