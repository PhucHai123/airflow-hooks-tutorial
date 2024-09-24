# Research on Airflow Hooks and Operators

This repository contains research and experimentation on using Airflow hooks and custom operators to interact with various external services.

## Overview

Airflow hooks and custom operators allow developers to extend Airflow's capabilities by interacting with external services. Hooks allow you to interact with external services from within Airflow, while custom operators allow you to write custom tasks that can be used in Airflow DAGs.

## Research areas

### S3 Hooks

* [x] Research on using the `S3KeyHook` to download files from S3
* [x] Research on using the `S3KeySensor` to detect when a file is uploaded to S3

### Custom Operators

* [x] Research on creating a custom operator to interact with an external service (e.g. a REST API)

## Why?

Airflow provides a great way to orchestrate workflows, but sometimes you need to interact with external services that are not directly supported by Airflow. By creating custom hooks and operators, you can extend Airflow's capabilities and interact with a wider range of external services.

## How?

This research is being done using the following tools:

* Airflow: The open-source platform for programmatically defining, scheduling, and monitoring workflows.
* Docker: A containerization platform for running Airflow and other services.
* Docker Compose: A tool for defining and running multi-container Docker applications.
* Jupyter Notebook: A web-based interactive computing environment. Used for researching and testing hooks and operators.

## Conclusion

This repository contains research and experimentation on using Airflow hooks and custom operators to interact with various external services. The goal of this research is to provide a comprehensive guide on how to create custom hooks and operators for Airflow, and to demonstrate how they can be used to interact with external services.
