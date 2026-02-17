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
package com.datasqrl.env;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Internally used environment variable names. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EnvVariableNames {

  public static final String DUCKDB_EXTENSIONS_DIR = "DUCKDB_EXTENSIONS_DIR";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS";
  public static final String KAFKA_REGISTRY_URL = "KAFKA_REGISTRY_URL";
  public static final String KAFKA_GROUP_ID = "KAFKA_GROUP_ID";

  public static final String POSTGRES_VERSION = "POSTGRES_VERSION";
  public static final String POSTGRES_HOST = "POSTGRES_HOST";
  public static final String POSTGRES_PORT = "POSTGRES_PORT";
  public static final String POSTGRES_DATABASE = "POSTGRES_DATABASE";
  public static final String POSTGRES_AUTHORITY = "POSTGRES_AUTHORITY";
  public static final String POSTGRES_JDBC_URL = "POSTGRES_JDBC_URL";
  public static final String POSTGRES_USERNAME = "POSTGRES_USERNAME";
  public static final String POSTGRES_PASSWORD = "POSTGRES_PASSWORD";
}
