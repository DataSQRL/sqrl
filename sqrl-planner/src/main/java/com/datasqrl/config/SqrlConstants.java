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
package com.datasqrl.config;

import java.nio.file.Path;

public class SqrlConstants {

  public static final String BUILD_DIR_NAME = "build";
  public static final String DEPLOY_DIR_NAME = "deploy";
  public static final String PLAN_DIR = "plan";
  public static final Path PLAN_PATH = Path.of(BUILD_DIR_NAME, DEPLOY_DIR_NAME, PLAN_DIR);
  public static final String PACKAGE_JSON = "package.json";
  public static final Path DEFAULT_PACKAGE = Path.of(PACKAGE_JSON);
  public static final String FLINK_ASSETS_DIR = "flink";
  public static final String LIB_DIR = "lib";
  public static final String DATA_DIR = "data";
  public static final String SQRL_EXTENSION = "sqrl";
  public static final String SQL_EXTENSION = "sql";
  public static final String GRAPHQL_EXTENSION = "graphql";
  public static final String GRAPHQL_SCHEMA_EXTENSION = "graphqls";
}
