# SQRL Server

The implementation of the server component of DataSQRL. The server is responsible for exposing
the API and translating API queries, subscriptions, and mutations into calls against the configured
data systems of the pipeline (Postgres, Kafka, etc).

The goal of the server implementation is to be very lean and efficient, using modern reactive Java
libraries to minimize concurrency overhead.

The server implementation is split into a generic GraphQL servlet implementation and
a specific implementation that uses Vertx as the server engine.

