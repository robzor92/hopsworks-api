# Hopsworks Feature Store

<p align="center">
  <a href="https://community.hopsworks.ai"><img
    src="https://img.shields.io/discourse/users?label=Hopsworks%20Community&server=https%3A%2F%2Fcommunity.hopsworks.ai"
    alt="Hopsworks Community"
  /></a>
    <a href="https://docs.hopsworks.ai"><img
    src="https://img.shields.io/badge/docs-HSFS-orange"
    alt="Hopsworks Feature Store Documentation"
  /></a>
  <a href="https://pypi.org/project/hsfs/"><img
    src="https://img.shields.io/pypi/v/hsfs?color=blue"
    alt="PyPiStatus"
  /></a>
  <a href="https://archiva.hops.works/#artifact/com.logicalclocks/hsfs"><img
    src="https://img.shields.io/badge/java-HSFS-green"
    alt="Scala/Java Artifacts"
  /></a>
  <a href="https://pepy.tech/project/hsfs/month"><img
    src="https://pepy.tech/badge/hsfs/month"
    alt="Downloads"
  /></a>
  <a href="https://github.com/psf/black"><img
    src="https://img.shields.io/badge/code%20style-black-000000.svg"
    alt="CodeStyle"
  /></a>
  <a><img
    src="https://img.shields.io/pypi/l/hsfs?color=green"
    alt="License"
  /></a>
</p>

HOPSWORKS is the library to interact with Hopsworks services.

The library automatically configures itself based on the environment it is run.
However, to connect from an external environment such as Databricks, AWS Sagemaker or a CI/CD environment additional connection information, such as host and port, is required. For more information about the setup from external environments, see the setup section.

## Getting Started On Hopsworks

Instantiate a connection and get the project object
```python
import hopsworks

connection = hopsworks.connection()

project_api = connection.get_project_api()

project = project_api.get_project("demo")
```

Create a new project
```python
project = project_api.create_project("demo")
```

You can find more examples on how to use the library in our [hops-examples](https://github.com/logicalclocks/hops-examples) repository.

## Documentation

Documentation is available at [Hopsworks Feature Store Documentation](https://docs.hopsworks.ai/).

## Issues

For general questions about the usage of Hopsworks and the Feature Store please open a topic on [Hopsworks Community](https://community.hopsworks.ai/).

Please report any issue using [Github issue tracking](https://github.com/logicalclocks/hopsworks-api/issues).

## Contributing

If you would like to contribute to this library, please see the [Contribution Guidelines](CONTRIBUTING.md).

