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

import java.util.Map;
import java.util.regex.Pattern;

/** Temporary wrapper around jdbc engine config until it can move to a template */
public class JdbcEngineConfigDelegate {
  public static final Pattern JDBC_URL_REGEX =
      Pattern.compile("^jdbc:(.*?):\\/\\/([^/:]*)(?::(\\d+))?\\/([^/:?]*)(.*)$");
  public static final Pattern JDBC_DIALECT_REGEX = Pattern.compile("^jdbc:(.*?):(.*)$");

  private final Map<String, String> map;
  private final String dialect;
  private String host;
  private int port;
  private String database;
  private final String url;

  public JdbcEngineConfigDelegate(ConnectorConf connectorConf) {
    this.map = connectorConf.toMap();
    this.url = map.get("url");
    var matcher = JDBC_URL_REGEX.matcher(url);
    if (matcher.find()) {
      var dialect = matcher.group(1);
      //      connectorConfig.getErrorCollector().checkFatal(JdbcDialect.find(dialect).isPresent(),
      // "Invalid database dialect: %s", dialect);
      this.dialect = dialect;
      this.host = matcher.group(2);
      this.port = Integer.parseInt(matcher.group(3));
      this.database = matcher.group(4);
    } else {
      // Only extract the dialect
      matcher = JDBC_DIALECT_REGEX.matcher(url);
      if (matcher.find()) {
        var dialect = matcher.group(1);
        this.dialect = dialect;
      } else {
        throw new RuntimeException("Invalid database URL: %s".formatted(url));
      }
    }
  }

  public JdbcDialect getDialect() {
    return JdbcDialect.find(dialect.toLowerCase()).get();
  }

  public String getHost() {
    return this.host;
  }

  public Integer getPort() {
    return this.port;
  }

  public String getUser() {
    return (String) map.get("username");
  }

  public String getPassword() {
    return (String) map.get("password");
  }

  public String getDatabase() {
    return this.database;
  }

  public String getUrl() {
    return this.url;
  }
}
