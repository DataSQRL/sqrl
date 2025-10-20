# Templating

DataSQRL uses the Mustache templating engine to substitute configuration variables in SQRL scripts, making them reusable and configurable without modifying the code.

## How It Works

Variables in your SQRL script are wrapped in double curly braces `{{variableName}}`.
When DataSQRL compiles the script, it replaces these placeholders with values defined in the `script.config` section of your [`package.json`](../configuration) configuration file.

## Example

### Configuration File (package.json)

```json
{
  "version": "1",
  "script": {
    "main": "query.sqrl",
    "config": {
      "tableName": "Users",
      "idColumn": "user_id",
      "timestampColumn": "created_at",
      "minAge": 18
    }
  }
}
```

### SQRL Script (query.sqrl)

```sql
IMPORT tables.{{tableName}};

FilteredUsers :=
SELECT {{idColumn}},
       name,
       {{timestampColumn}}
FROM {{tableName}}
WHERE age >= {{minAge}};
```

### After Substitution

When DataSQRL processes this script, it replaces all `{{variableName}}` placeholders:

```sql
IMPORT tables.Users;

FilteredUsers :=
SELECT user_id,
       name,
       created_at
FROM Users
WHERE age >= 18;
```

## Benefits

1. **Reusability**: The same SQRL script can work with different tables and columns by changing the config
2. **Maintainability**: Configuration is centralized in one place (package.json)
3. **Type Safety**: You can specify types like `partitionColType: "bigint"` and use them in the script
4. **Environment-Specific**: Easy to have different configs for dev, test, and production

## Special Variables

DataSQRL also provides built-in variables:
- `${DEPLOYMENT_ID}`: Unique identifier for each deployment
- `${DEPLOYMENT_TIMESTAMP}`: Timestamp when the job was deployed

These are substituted at deployment time and are useful for tracking and versioning.