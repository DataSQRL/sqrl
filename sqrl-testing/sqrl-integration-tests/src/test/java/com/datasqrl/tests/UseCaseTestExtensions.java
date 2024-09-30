package com.datasqrl.tests;


public class UseCaseTestExtensions {

  public TestExtension create(String testName) {
    if (testName.equals("snowflake")) {
      return new SnowflakeTestExtension();
    }
    if (testName.equals("duckdb")) {
      return new DuckdbTestExtension();
    }
    return TestExtension.NOOP;
  }

}
