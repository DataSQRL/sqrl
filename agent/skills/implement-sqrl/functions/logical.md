# Logical Functions

| SQL | Description |
|-----|-------------|
| `boolean1 OR boolean2` | Returns TRUE if BOOLEAN1 is TRUE or BOOLEAN2 is TRUE. Supports three-valued logic. E.g., true \|\| Null(BOOLEAN) returns TRUE. |
| `boolean1 AND boolean2` | Returns TRUE if BOOLEAN1 and BOOLEAN2 are both TRUE. Supports three-valued logic. E.g., true && Null(BOOLEAN) returns UNKNOWN. |
| `NOT boolean` | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE; returns UNKNOWN if boolean is UNKNOWN. |
| `boolean IS FALSE` | Returns TRUE if boolean is FALSE; returns FALSE if boolean is TRUE or UNKNOWN. |
| `boolean IS NOT FALSE` | Returns TRUE if BOOLEAN is TRUE or UNKNOWN; returns FALSE if BOOLEAN is FALSE. |
| `boolean IS TRUE` | Returns TRUE if BOOLEAN is TRUE; returns FALSE if BOOLEAN is FALSE or UNKNOWN. |
| `boolean IS NOT TRUE` | Returns TRUE if boolean is FALSE or UNKNOWN; returns FALSE if boolean is TRUE. |
| `boolean IS UNKNOWN` | Returns TRUE if boolean is UNKNOWN; returns FALSE if boolean is TRUE or FALSE. |
| `boolean IS NOT UNKNOWN` | Returns TRUE if boolean is TRUE or FALSE; returns FALSE if boolean is UNKNOWN. |
