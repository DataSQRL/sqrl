DataSQRL failed to read from or write to the local file system.

Check the message above for the affected path. Make sure the file exists, the DataSQRL process has permission to read and write the project and build directories, and, when running in Docker, the project directory is mounted correctly (e.g. `-v $PWD:/build`).
