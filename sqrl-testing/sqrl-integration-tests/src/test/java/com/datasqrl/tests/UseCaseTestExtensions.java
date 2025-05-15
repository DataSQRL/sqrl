/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
