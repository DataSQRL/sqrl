The sink used in an `EXPORT` statement or for debugging cannot
be found.

The sink name is either misspelled or the sink does not exist.
If the sink does not exist, you can define a sink locally, declare
it as a dependency, or use the `print` sink to print the output
to stdout.

For example:
```
EXPORT MyTable TO print.mytable;
```