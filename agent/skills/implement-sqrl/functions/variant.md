# Variant Functions

| SQL | Description |
|-----|-------------|
| `PARSE_JSON(json_string[, allow_duplicate_keys])` | Parse a JSON string into a Variant. If the JSON string is invalid, an error will be thrown.  To return NULL instead of an error, use the `TRY_PARSE_JSON` function.  If there are duplicate keys in the input JSON string, when `allowDuplicateKeys` is true, the  parser will keep the last occurrence of all fields with the same key, otherwise when  `allowDuplicateKeys` is false it will throw an error. The default value of  `allowDuplicateKeys` is false.  |
| `TRY_PARSE_JSON(json_string[, allow_duplicate_keys])` | Try to parse a JSON string into a Variant if possible. If the JSON string is invalid, return  NULL. To throw an error instead of returning NULL, use the `PARSE_JSON` function.  If there are duplicate keys in the input JSON string, when `allowDuplicateKeys` is true, the  parser will keep the last occurrence of all fields with the same key, otherwise when  `allowDuplicateKeys` is false it will throw an error. The default value of  `allowDuplicateKeys` is false.  |
