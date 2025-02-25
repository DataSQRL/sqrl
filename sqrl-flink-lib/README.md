# SQRL Flink Lib

The Flink library modules are flink runtime modules for connectors, formats, and functions
that DataSQRL adds to Flink.

Specifically, DataSQRL adds the following functionality to Flink:
* Flexible version of the Json and CSV format that are more robust to errors in the input data
* Additional datatypes like JSON and Vector
* Dialect specific JDBC implementations for JDBC sinks that support additional data types
* Various function libraries with additional standard functions that DataSQRL includes.