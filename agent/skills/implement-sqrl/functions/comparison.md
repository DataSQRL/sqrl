# Comparison Functions

| SQL | Description |
|-----|-------------|
| `value1 = value2` | Returns TRUE if value1 is equal to value2; returns UNKNOWN if value1 or value2 is NULL. |
| `value1 <> value2` | Returns TRUE if value1 is not equal to value2; returns UNKNOWN if value1 or value2 is NULL. |
| `value1 > value2` | Returns TRUE if value1 is greater than value2; returns UNKNOWN if value1 or value2 is NULL. |
| `value1 >= value2` | Returns TRUE if value1 is greater than or equal to value2; returns UNKNOWN if value1 or value2 is NULL. |
| `value1 < value2` | Returns TRUE if value1 is less than value2; returns UNKNOWN if value1 or value2 is NULL. |
| `value1 <= value2` | Returns TRUE if value1 is less than or equal to value2; returns UNKNOWN if value1 or value2 is NULL. |
| `value IS NULL` | Returns TRUE if value is NULL. |
| `value IS NOT NULL` | Returns TRUE if value is not NULL. |
| `value1 IS DISTINCT FROM value2` | Returns TRUE if two values are different. NULL values are treated as identical here. E.g., 1 IS DISTINCT FROM NULL returns TRUE; NULL IS DISTINCT FROM NULL returns FALSE. |
| `value1 IS NOT DISTINCT FROM value2` | Returns TRUE if two values are equal. NULL values are treated as identical here. E.g., 1 IS NOT DISTINCT FROM NULL returns FALSE; NULL IS NOT DISTINCT FROM NULL returns TRUE. |
| `value1 BETWEEN [ ASYMMETRIC \| SYMMETRIC ] value2 AND value3` | By default (or with the ASYMMETRIC keyword), returns TRUE if value1 is greater than or equal to value2 and less than or equal to value3. With the SYMMETRIC keyword, returns TRUE if value1 is inclusively between value2 and value3. When either value2 or value3 is NULL, returns FALSE or UNKNOWN. E.g., 12 BETWEEN 15 AND 12 returns FALSE; 12 BETWEEN SYMMETRIC 15 AND 12 returns TRUE; 12 BETWEEN 10 AND NULL returns UNKNOWN; 12 BETWEEN NULL AND 10 returns FALSE; 12 BETWEEN SYMMETRIC NULL AND 12 returns UNKNOWN. |
| `value1 NOT BETWEEN [ ASYMMETRIC \| SYMMETRIC ] value2 AND value3` | By default (or with the ASYMMETRIC keyword), returns TRUE if value1 is less than value2 or greater than value3. With the SYMMETRIC keyword, returns TRUE if value1 is not inclusively between value2 and value3. When either value2 or value3 is NULL, returns TRUE or UNKNOWN. E.g., 12 NOT BETWEEN 15 AND 12 returns TRUE; 12 NOT BETWEEN SYMMETRIC 15 AND 12 returns FALSE; 12 NOT BETWEEN NULL AND 15 returns UNKNOWN; 12 NOT BETWEEN 15 AND NULL returns TRUE; 12 NOT BETWEEN SYMMETRIC 12 AND NULL returns UNKNOWN. |
| `string1 LIKE string2 [ ESCAPE char ]` | Returns TRUE if string1 matches pattern string2; returns UNKNOWN if string1 or string2 is NULL. An escape character consisting of a single char can be defined if necessary, '\' by default. |
| `string1 NOT LIKE string2 [ ESCAPE char ]` | Returns TRUE if string1 does not match pattern string2; returns UNKNOWN if string1 or string2 is NULL. An escape character consisting of a single char can be defined if necessary, '\' by default. |
| `string1 SIMILAR TO string2 [ ESCAPE char ]` | Returns TRUE if string1 matches SQL regular expression string2; returns UNKNOWN if string1 or string2 is NULL. An escape character can be defined if necessary. The escape character has not been supported yet. |
| `string1 NOT SIMILAR TO string2 [ ESCAPE char ]` | Returns TRUE if string1 does not match SQL regular expression string2; returns UNKNOWN if string1 or string2 is NULL. An escape character can be defined if necessary. The escape character has not been supported yet. |
| `value1 IN (value2 [, value3]* )` | Returns TRUE if value1 exists in the given list (value2, value3, ...). When (value2, value3, ...). contains NULL, returns TRUE if the element can be found and UNKNOWN otherwise. Always returns UNKNOWN if value1 is NULL. E.g., 4 IN (1, 2, 3) returns FALSE; 1 IN (1, 2, NULL) returns TRUE; 4 IN (1, 2, NULL) returns UNKNOWN. |
| `value1 NOT IN (value2 [, value3]* )` | Returns TRUE if value1 does not exist in the given list (value2, value3, ...). When (value2, value3, ...). contains NULL, returns FALSE if value1 can be found and UNKNOWN otherwise. Always returns UNKNOWN if value1 is NULL. E.g., 4 NOT IN (1, 2, 3) returns TRUE; 1 NOT IN (1, 2, NULL) returns FALSE; 4 NOT IN (1, 2, NULL) returns UNKNOWN. |
| `EXISTS (sub-query)` | Returns TRUE if sub-query returns at least one row. Only supported if the operation can be rewritten in a join and group operation. For streaming queries the operation is rewritten in a join and group operation. The required state to compute the query result might grow infinitely depending on the number of distinct input rows. Please provide a query configuration with valid retention interval to prevent excessive state size. |
| `value IN (sub-query)` | Returns TRUE if value is equal to a row returned by sub-query. |
| `value NOT IN (sub-query)` | Returns TRUE if value is not equal to a row returned by sub-query. |

