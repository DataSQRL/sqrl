The packager prepares the build directory for the compiler. The job of the
packager is to populate the `build` directory with all the files needed by the
compiler, so that the compiler can operate exclusively on the build directory and
have all the files it needs prepared in a standard way.

The packager
resolves external data and function dependencies that are imported in a script.
Those are either resolved against the local filesystem or downloaded from a repository
and placed into the build directory.

The packager also runs all registered pre-processors on the local directory
to pre-process input files and place them into the build directory for the
compiler. DataSQRL has a generic pre-processor framework.
