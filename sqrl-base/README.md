# SQRL Base

All sqrl modules except sqrl-flink-lib depend on this base module. 
The base module contains core functionality and interfaces that are shared by the compiler, planner, and server.

The base module contains the minimal set of dependencies that all of these modules inherit.
The base module should remain as lean as possible since modules like `sqrl-server-vertex` need to be very lightweight.

If something is not needed on the server, it should live in sqrl-tools or sqrl-planner instead of this base module.