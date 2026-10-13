DataSQRL could not parse or convert a schema file, such as an Avro schema loaded with ``LIKE `user.avsc` ``.

Besides malformed schema files, this happens for Avro constructs that cannot be mapped to SQL types: unions with more than one non-null type, recursive (self-referencing) records, and unsupported types.

Erroneous Avro schema example:
```json
{
  "type": "record",
  "name": "User",
  "fields": [
    { "name": "id", "type": ["null", "long", "string"] }
  ]
}
```

Use unions with a single non-null type, e.g. `["null", "long"]`, avoid recursive records, and check the message above for the path of the offending field.
