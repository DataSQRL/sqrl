# Hashfunctions Functions

| SQL | Description |
|-----|-------------|
| `MD5(string)` | Returns the MD5 hash of string as a string of 32 hexadecimal digits; returns NULL if string is NULL. |
| `SHA1(string)` | Returns the SHA-1 hash of string as a string of 40 hexadecimal digits; returns NULL if string is NULL. |
| `SHA224(string)` | Returns the SHA-224 hash of string as a string of 56 hexadecimal digits; returns NULL if string is NULL. |
| `SHA256(string)` | Returns the SHA-256 hash of string as a string of 64 hexadecimal digits; returns NULL if string is NULL. |
| `SHA384(string)` | Returns the SHA-384 hash of string as a string of 96 hexadecimal digits; returns NULL if string is NULL. |
| `SHA512(string)` | Returns the SHA-512 hash of string as a string of 128 hexadecimal digits; returns NULL if string is NULL. |
| `SHA2(string, hashLength)` | Returns the hash using the SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, or SHA-512). The first argument string is the string to be hashed and the second argument hashLength is the bit length of the result (224, 256, 384, or 512). Returns NULL if string or hashLength is NULL. |
