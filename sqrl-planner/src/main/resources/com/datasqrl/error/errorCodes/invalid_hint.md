A hint comment block contains something other than comma-separated hints.

A hint block has the form `/*+ hint_without_args, hint_with_args(arg1, arg2) */` and must not contain anything else, including other comments.

Erroneous code example:
```sql
/*+ primary_key(customerid) index(HASH, email) */
CustomerInfo := SELECT customerid, email, name FROM Customer;
```

Separate hints with commas, and move any other comments into a separate comment block:
```sql
/*+ primary_key(customerid), index(HASH, email) */
CustomerInfo := SELECT customerid, email, name FROM Customer;
```
