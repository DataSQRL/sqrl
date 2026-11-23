The data contract does not match the table.

A schema object lists the physical columns of the `CREATE TABLE` of the same name, with the same
`logicalType` and `required`, and the tags and classification that the table and its columns
declare. Change the contract or the table so that they match. The compiler writes the contract
that it generates for each table to `build/deploy/contracts`.
