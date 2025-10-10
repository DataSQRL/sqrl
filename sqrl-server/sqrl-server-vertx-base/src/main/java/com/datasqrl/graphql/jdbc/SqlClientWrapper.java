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
package com.datasqrl.graphql.jdbc;

import io.vertx.sqlclient.SqlClient;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SqlClientWrapper {

  private final SqlClient sqlClient;

  public static class DuckDbClientWrapper extends SqlClientWrapper {

    private final Map<String, Object> config;

    public DuckDbClientWrapper(SqlClient sqlClient, Map<String, Object> config) {
      super(sqlClient);
      this.config = config;
    }

    public DuckDbClientWrapper withClient(SqlClient sqlClient) {
      return new DuckDbClientWrapper(sqlClient, config);
    }

    public String getExtensionInstall() {
      var extInstallStr = "INSTALL iceberg;";
      if (useDiskCache()) {
        extInstallStr += "INSTALL cache_httpfs FROM community;";
      }

      return extInstallStr;
    }

    public String getExtensionLoad() {
      var extLoadStr = "LOAD iceberg;";
      if (useDiskCache()) {
        extLoadStr += "LOAD cache_httpfs;";
      }

      return extLoadStr;
    }

    private boolean useDiskCache() {
      return (boolean) config.getOrDefault("use-disk-cache", false);
    }
  }
}
