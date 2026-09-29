# Aggregate Functions

| SQL | Description |
|-----|-------------|
| `COUNT([ ALL ] expression \| DISTINCT expression1 [, expression2]*)` | By default or with ALL, returns the number of input rows for which expression is not NULL. Use DISTINCT for one unique instance of each value. |
| `COUNT(*) COUNT(1) ` | Returns the number of input rows. |
| `AVG([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns the average (arithmetic mean) of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `SUM([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns the sum of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `MAX([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns the maximum value of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `MIN([ ALL \| DISTINCT ] expression )` | By default or with keyword ALL, returns the minimum value of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `STDDEV_POP([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns the population standard deviation of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `STDDEV_SAMP([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns the sample standard deviation of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `VAR_POP([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns the population variance (square of the population standard deviation) of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `VAR_SAMP([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns the sample variance (square of the sample standard deviation) of expression across all input rows. Use DISTINCT for one unique instance of each value. |
| `COLLECT([ ALL \| DISTINCT ] expression)` | By default or with keyword ALL, returns a multiset of expression across all input rows. NULL values will be ignored. Use DISTINCT for one unique instance of each value. |
| `VARIANCE([ ALL \| DISTINCT ] expression)` | Synonyms for VAR_SAMP(). |
| `RANK()` | Returns the rank of a value in a group of values. The result is one plus the number of rows preceding or equal to the current row in the ordering of the partition. The values will produce gaps in the sequence. |
| `DENSE_RANK()` | Returns the rank of a value in a group of values. The result is one plus the previously assigned rank value. Unlike the function rank, dense_rank will not produce gaps in the ranking sequence. |
| `ROW_NUMBER()` | Assigns a unique, sequential number to each row, starting with one, according to the ordering of rows within the window partition. ROW_NUMBER and RANK are similar. ROW_NUMBER numbers all rows sequentially (for example 1, 2, 3, 4, 5). RANK provides the same numeric value for ties (for example 1, 2, 2, 4, 5). |
| `LEAD(expression [, offset] [, default])` | Returns the value of expression at the offsetth row after the current row in the window. The default value of offset is 1 and the default value of default is NULL. |
| `LAG(expression [, offset] [, default])` | Returns the value of expression at the offsetth row before the current row in the window. The default value of offset is 1 and the default value of default is NULL. |
| `FIRST_VALUE(expression)` | Returns the first value in an ordered set of values. |
| `LAST_VALUE(expression)` | Returns the last value in an ordered set of values. |
| `LISTAGG(expression [, separator])` | Concatenates the values of string expressions and places separator values between them. The separator is not added at the end of string. The default value of separator is ','. |
| `CUME_DIST()` | Return the cumulative distribution of a value in a group of values. The result is the number of rows preceding or equal to the current row in the ordering of the partition divided by the number of rows in the window partition. |
| `PERCENT_RANK()` | Return the percentage ranking of a value in a group of values. The result is the rank value minus one, divided by the number of rows in the parition minus one. If the partition only contains one row, the function will return 0. |
| `NTILE(n)` | Divides the rows for each window partition into `n` buckets ranging from 1 to at most `n`. If the number of rows in the window partition doesn't divide evenly into the number of buckets, then the remainder values are distributed one per bucket, starting with the first bucket. For example, with 6 rows and 4 buckets, the bucket values would be as follows: 1 1 2 2 3 4  |
| `ARRAY_AGG([ ALL \| DISTINCT ] expression [ RESPECT NULLS \| IGNORE NULLS ])` | By default or with keyword `ALL` and, return an array that concatenates the input rows and returns `NULL` if there are no input rows. Use `DISTINCT` for one unique instance of each value. By default `NULL` values are respected, use `IGNORE NULLS` to skip `NULL` values. The `ORDER BY` clause is currently not supported.  |
