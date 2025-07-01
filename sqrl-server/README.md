# SQRL Server

The implementation of the server component of DataSQRL. The server is responsible for exposing
the API and translating API queries, subscriptions, and mutations into calls against the configured
data systems of the pipeline (Postgres, Kafka, etc).

The goal of the server implementation is to be very lean and efficient, using modern reactive Java
libraries to minimize concurrency overhead.

The server implementation is split into a "core" module that defines the interface to the compiler (i.e. the classes that get generated) and "base" which contains the servlet implementation for Vert.x.
The "vertx" module is a lightweight module for a standalone deployment with Dockerfile.

