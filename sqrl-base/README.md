All sqrl modules depend on this base module. The base module contains core functionality and interfaces that are shared by all modules.

The base module contains the minimal set of dependencies that all other modules inherit.

The base module should remain as lean as possible since it is modules like `sqrl-server-vertex` that need to be lightweight. 
Most shared classes and interfaces should live in `sqrl-common`.