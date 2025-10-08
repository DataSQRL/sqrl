# Snowflake Engine Configuration

Snowflake is a cloud-based analytic database query engine that can read Iceberg tables and provide enterprise-scale analytical capabilities.

## Configuration Options

| Key               | Type       | Default | Description                          |
|-------------------|------------|---------|--------------------------------------|
| `catalog-name`    | **string** | –       | Glue catalog name for metadata       |
| `external-volume` | **string** | –       | Snowflake external volume name       |
| `url`             | **string** | –       | Full JDBC URL including auth params  |

## Example Configuration

```json
{
  "engines": {
    "snowflake": {
      "catalog-name": "my-glue-catalog",
      "external-volume": "my-external-volume", 
      "url": "jdbc:snowflake://account.snowflakecomputing.com/?user=username&password=password&warehouse=warehouse&db=database&schema=schema"
    }
  }
}
```

## Usage Notes

- Requires all three configuration parameters
- Designed for large-scale analytical workloads in the cloud
- Integrates with AWS Glue for metadata management
- Uses external volumes for accessing Iceberg data
- Authentication parameters should be included in the JDBC URL
- For local development, consider using DuckDB as a substitute
- Provides enterprise features like data sharing and governance