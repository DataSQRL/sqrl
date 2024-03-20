The SELECT clause of the query selects the same primary key column
multiple times, but only the first reference will be used to
construct the resulting table's primary key.

Note, that you can only use the first reference in operators
that requires a primary key (such as a temporal join).

If you intend to select the primary key multiple times and don't plan to use
subsequent references as primary keys in the following statements, you can
ignore this warning.