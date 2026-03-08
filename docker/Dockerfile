FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    gnupg \
    unixodbc \
    unixodbc-dev

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -

RUN curl https://packages.microsoft.com/config/debian/11/prod.list \
    > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

USER airflow