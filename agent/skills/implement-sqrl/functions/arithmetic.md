# Arithmetic Functions

| SQL | Description |
|-----|-------------|
| `+ numeric` | Returns NUMERIC. |
| `- numeric` | Returns negative Numeric |
| `numeric1 + numeric2` | Returns NUMERIC1 plus NUMERIC2. |
| `numeric1 - numeric2` | Return NUMERIC1 minus NUMERIC2 |
| `numeric1 * numberic2` | Returns NUMERIC1 multiplied by NUMERIC2 |
| `numeric1 / numeric2` | Returns NUMERIC1 divided by NUMERIC2 |
| `numeric1 % numeric2` | Returns the remainder (modulus) of numeric1 divided by numeric2. The result is negative only if numeric1 is negative. |
| `POWER(numeric1, numeric2)` | NUMERIC1.power(NUMERIC2) |
| `ABS(numeric)` | Returns the absolute value of numeric. |
| `SQRT(numeric)` | Returns the square root of NUMERIC. |
| `LN(numeric)` | Returns the natural logarithm (base e) of NUMERIC. |
| `LOG10(numeric)` | Returns the base 10 logarithm of numeric. |
| `LOG2(numeric)` | Returns the base 2 logarithm of numeric. |
| `LOG(numeric2) LOG(numeric1, numeric2) ` | When called with one argument, returns the natural logarithm of numeric2. When called with two arguments, this function returns the logarithm of numeric2 to the base numeric1. Currently, numeric2 must be greater than 0 and numeric1 must be greater than 1. |
| `EXP(numeric)` | Returns e raised to the power of numeric. |
| `CEIL(numeric) CEILING(numeric) ` | Rounds numeric up, and returns the smallest number that is greater than or equal to numeric. |
| `FLOOR(numeric)` | Rounds numeric down, and returns the largest number that is less than or equal to numeric. |
| `SIN(numeric)` | Returns the sine of numeric. |
| `SINH(numeric)` | Returns the hyperbolic sine of numeric. The return type is DOUBLE. |
| `COS(numeric)` | Returns the cosine of numeric. |
| `TAN(numeric)` | Returns the tangent of numeric. |
| `TANH(numeric)` | Returns the hyperbolic tangent of numeric. The return type is DOUBLE. |
| `COT(numeric)` | Returns the cotangent of a numeric. |
| `ASIN(numeric)` | Returns the arc sine of numeric. |
| `ACOS(numeric)` | Returns the arc cosine of numeric. |
| `ATAN(numeric)` | Returns the arc tangent of numeric. |
| `ATAN2(numeric1, numeric2)` | Returns the arc tangent of a coordinate (NUMERIC1, NUMERIC2). |
| `COSH(numeric)` | Returns the hyperbolic cosine of NUMERIC. Return value type is DOUBLE. |
| `DEGREES(numeric)` | Returns the degree representation of a radian NUMERIC. |
| `RADIANS(numeric)` | Returns the radian representation of a degree NUMERIC. |
| `SIGN(numeric)` | Returns the signum of NUMERIC. |
| `ROUND(NUMERIC, INT)` | Returns a number rounded to INT decimal places for NUMERIC. |
| `PI()` | Returns a value that is closer than any other values to pi. |
| `E()` | Returns a value that is closer than any other values to e. |
| `RAND()` | Returns a pseudorandom double value in the range [0.0, 1.0) |
| `RAND(INT)` | Returns a pseudorandom double value in the range [0.0, 1.0) with an initial seed integer. Two RAND functions will return identical sequences of numbers if they have the same initial seed. |
| `RAND_INTEGER(INT)` | Returns a pseudorandom integer value in the range [0, INT) |
| `RAND_INTEGER(INT1, INT2)` | Returns a pseudorandom integer value in the range [0, INT2) with an initial seed INT1. Two RAND_INTGER functions will return idential sequences of numbers if they have the same initial seed and bound. |
| `UUID()` | Returns an UUID (Universally Unique Identifier) string (e.g., "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly generated) UUID. The UUID is generated using a cryptographically strong pseudo random number generator. |
| `BIN(INT)` | Returns a string representation of INTEGER in binary format. Returns NULL if INTEGER is NULL. E.g., 4.bin() returns "100" and 12.bin() returns "1100". |
| `HEX(numeric) HEX(string) ` | Returns a string representation of an integer NUMERIC value or a STRING in hex format. Returns NULL if the argument is NULL. E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", a string "hello,world" leads to "68656C6C6F2C776F726C64". |
| `UNHEX(expr)` | Converts hexadecimal string expr to BINARY. If the length of expr is odd, the first character is discarded and the result is left padded with a null byte.   E.g., SELECT DECODE(UNHEX('466C696E6B') , 'UTF-8' ) or '466C696E6B'.unhex().decode('UTF-8') returns "Flink".  expr <CHAR \| VARCHAR>  Returns a BINARY. `NULL` if expr is `NULL` or expr contains non-hex characters.  |
| `TRUNCATE(numeric1, integer2)` | Returns a numeric of truncated to integer2 decimal places. Returns NULL if numeric1 or integer2 is NULL. If integer2 is 0, the result has no decimal point or fractional part. integer2 can be negative to cause integer2 digits left of the decimal point of the value to become zero. This function can also pass in only one numeric1 parameter and not set integer2 to use. If integer2 is not set, the function truncates as if integer2 were 0. E.g. 42.324.truncate(2) to 42.32. and 42.324.truncate() to 42.0. |
| `PERCENTILE(expr, percentage[, frequency])` | Returns the percentile value of expr at the specified percentage using continuous distribution.  E.g., SELECT PERCENTILE(age, 0.25) FROM (VALUES  (10), (20), (30), (40)) AS age or $('age').percentile(0.25) returns 17.5  The percentage must be a literal numeric value between `[0.0, 1.0]` or an array of such values.  If a variable expression is passed to this function, the result will be calculated using any one of them. frequency describes how many times expr should be counted, the default value is 1.  If no expr lies exactly at the desired percentile, the result is calculated using linear interpolation of the two nearest exprs.  If expr or frequency is `NULL`, or frequency is not positive, the input row will be ignored.  NOTE: It is recommended to use this function in a window scenario, as it typically offers better performance.  In a regular group aggregation scenario, users should be aware of the performance overhead caused by a full sort triggered by each record.  `value <NUMERIC>, percentage [<NUMERIC NOT NULL> \| <ARRAY<NUMERIC NOT NULL> NOT NULL>], frequency <INTEGER_NUMERIC>` `(INTEGER_NUMERIC: TINYINT, SMALLINT, INTEGER, BIGINT)` `(NUMERIC: INTEGER_NUMERIC, FLOAT, DOUBLE, DECIMAL)`  Returns a `DOUBLE` if percentage is numeric, or an `ARRAY<DOUBLE>` if percentage is an array. `NULL` if percentage is an empty array.  |
