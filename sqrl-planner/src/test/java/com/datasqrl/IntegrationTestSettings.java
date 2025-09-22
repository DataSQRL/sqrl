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
package com.datasqrl;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class IntegrationTestSettings {
  public enum LogEngine {
    KAFKA,
    NONE
  }

  public enum StreamEngine {
    FLINK,
    INMEMORY,
    NONE
  }

  public enum DatabaseEngine {
    INMEMORY,
    H2,
    POSTGRES,
    SQLITE
  }

  public enum ServerEngine {
    VERTX
  }

  @Builder.Default final LogEngine log = LogEngine.NONE;
  @Builder.Default final StreamEngine stream = StreamEngine.INMEMORY;
  @Builder.Default final ServerEngine server = ServerEngine.VERTX;
  @Builder.Default final DatabaseEngine database = DatabaseEngine.POSTGRES;

  public static IntegrationTestSettings getInMemory() {
    return IntegrationTestSettings.builder().build();
  }

  public static IntegrationTestSettings getFlinkWithDB() {
    return getEngines(StreamEngine.FLINK, DatabaseEngine.POSTGRES).build();
  }

  public static IntegrationTestSettings.IntegrationTestSettingsBuilder getFlinkWithDBConfig() {
    return getEngines(StreamEngine.FLINK, DatabaseEngine.POSTGRES);
  }

  public static IntegrationTestSettings getFlinkWithDB(DatabaseEngine engine) {
    return getEngines(StreamEngine.FLINK, engine).build();
  }

  public static IntegrationTestSettings.IntegrationTestSettingsBuilder getEngines(
      StreamEngine stream, DatabaseEngine database) {
    return IntegrationTestSettings.builder().stream(stream).database(database);
  }

  public static IntegrationTestSettings getDatabaseOnly(DatabaseEngine database) {
    return getEngines(StreamEngine.NONE, database).build();
  }
}
