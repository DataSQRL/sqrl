The build tool contains a separate piece of functionality for data discovery.
The goal for data discovery is to make it easy for new users to get started
with DataSQRL by automatically generating table definitions for users' data.

This is implemented as a pre-processor that automatically extracts a schema
from `.jsonl` and `.csv` files and generates a table definition with connector
information for such files.

Discovery analyzes those files to produce data statistics that are used to determine the schema of the data. Based on the schema, discovery produces a table definition in the same directory as the data with connector configuration for reading the data file. This connector can then be imported into a script.