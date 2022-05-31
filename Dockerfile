FROM fedora:latest

WORKDIR /app

COPY . /app

RUN yum -y install postgresql
