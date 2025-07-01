# SQRL Server

The server module contains the default server implemented based on Vert.x.
The server takes a configuration file as input which maps each API entry point
to a single or set of SQL queries that are executed against the database on
request.

The server processes mutations by creating events and persisting them to a log.
The server handles subscriptions by listening to new log events and forwarding
them to clients.

The configuration file that defines the behavior of the server is a
deployment asset produced by the DataSQRL build tool for the "server" stage.

The goal of the server implementation is to be very lean and efficient, using modern reactive Java
libraries to minimize concurrency overhead.

The server implementation is split into a "core" module that defines the interface to the compiler (i.e. the classes that get generated) and "base" which contains the servlet implementation for Vert.x.
The "vertx" module is a lightweight module for a standalone deployment with Dockerfile.

