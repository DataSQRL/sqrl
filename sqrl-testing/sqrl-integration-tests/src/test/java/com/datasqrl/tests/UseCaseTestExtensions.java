package com.datasqrl.tests;


public class UseCaseTestExtensions {

  public TestExtension create(String testName) {
    if (testName.equals("snowflake")) {
      return new SnowflakeTestExtension();
    }
    if (testName.equals("duckdb") || testName.equals("analytics-only")) {
      return new DuckdbTestExtension();
    }
    return TestExtension.NOOP;
  }

}
