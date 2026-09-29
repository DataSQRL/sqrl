# Conditional Functions

| SQL | Description |
|-----|-------------|
| `CASE value   WHEN value1_1 [, value1_2]* THEN RESULT1   (WHEN value2_1 [, value2_2 ]* THEN result_2)*   (ELSE result_z) END ` | Returns resultX when the first time value is contained in (valueX_1, valueX_2, ...). When no value matches, returns result_z if it is provided and returns NULL otherwise. |
| `CASE    WHEN condition1 THEN result1   (WHEN condition2 THEN result2)*   (ELSE result_z) END ` | Returns resultX when the first conditionX is met. When no condition is met, returns result_z if it is provided and returns NULL otherwise. |
| `NULLIF(value1, value2)` | Returns NULL if value1 is equal to value2; returns value1 otherwise. E.g., NULLIF(5, 5) returns NULL; NULLIF(5, 0) returns 5. |
| `COALESCE(value1 [, value2]*)` | Returns the first argument that is not NULL.  If all arguments are NULL, it returns NULL as well. The return type is the least restrictive, common type of all of its arguments. The return type is nullable if all arguments are nullable as well.  ```sql -- Returns 'default' COALESCE(NULL, 'default')  -- Returns the first non-null value among f0 and f1, -- or 'default' if f0 and f1 are both NULL COALESCE(f0, f1, 'default') ```  |
| `IF(condition, true_value, false_value)` | Returns the true_value if condition is met, otherwise false_value. E.g., IF(5 > 3, 5, 3) returns 5. |
| `IFNULL(input, null_replacement)` | Returns null_replacement if input is NULL; otherwise input is returned.   Compared to COALESCE or CASE WHEN, this function returns a data type that is very specific in terms of nullability. The returned type is the common type of both arguments but only nullable if the null_replacement is nullable.  The function allows to pass nullable columns into a function or table that is declared with a NOT NULL constraint.  E.g., IFNULL(nullable_column, 5) returns never NULL.  |
| `IS_ALPHA(string)` | Returns true if all characters in string are letter, otherwise false. |
| `IS_DECIMAL(string)` | Returns true if string can be parsed to a valid numeric, otherwise false. |
| `IS_DIGIT(string)` | Returns true if all characters in string are digit, otherwise false. |
| `BOOLEAN.?(VALUE1, VALUE2)` | Returns VALUE1 if BOOLEAN evaluates to TRUE; returns VALUE2 otherwise. E.g., (42 > 5).?('A', 'B') returns "A". |
| `GREATEST(value1[, value2]*)` | Returns the greatest value of the list of arguments. Returns NULL if any argument is NULL. |
| `LEAST(value1[, value2]*)` | Returns the least value of the list of arguments. Returns NULL if any argument is NULL. |
