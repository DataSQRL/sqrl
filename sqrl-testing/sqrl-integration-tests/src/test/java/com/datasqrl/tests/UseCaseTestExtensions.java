package com.datasqrl.tests;


public class UseCaseTestExtensions {

  public TestExtension create(String scriptName) {
    if (scriptName.equals("snowflake")) {
      return new SnowflakeTestExtension();
    }
    if (scriptName.equals("duckdb") || scriptName.equals("analytics-only")) {
      return new DuckdbTestExtension();
    }
    if (scriptName.equals("iceberg-export")) {
      return new IcebergTestExtension();
    }
    return TestExtension.NOOP;
  }

}
