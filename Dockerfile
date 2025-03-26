# Use the official Airflow image as the base
FROM apache/airflow:latest

# Install the Docker provider for Airflow
RUN pip install apache-airflow-providers-docker

# Pulling python image with a specific tag from the docker hub.
FROM python:3.8-slim


# Install updated PostgreSQL and Cron command-line tools and packages
RUN apt-get update && apt-get install -y postgresql-client cron