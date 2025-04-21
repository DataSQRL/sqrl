package com.datasqrl.config;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Temporary wrapper around jdbc engine config until it can move to a template
 */
public class JdbcEngineConfigDelegate {
  public static final Pattern JDBC_URL_REGEX = Pattern.compile("^jdbc:(.*?):\\/\\/([^/:]*)(?::(\\d+))?\\/([^/:?]*)(.*)$");
  public static final Pattern JDBC_DIALECT_REGEX = Pattern.compile("^jdbc:(.*?):(.*)$");

  private final Map<String, Object> map;
  private final String dialect;
  private String host;
  private int port;
  private String database;
  private final String url;

  public JdbcEngineConfigDelegate(ConnectorConf connectorConf) {
    this.map = connectorConf.toMap();
    this.url = (String)map.get("url");
    Matcher matcher = JDBC_URL_REGEX.matcher(url);
    if (matcher.find()) {
      String dialect = matcher.group(1);
//      connectorConfig.getErrorCollector().checkFatal(JdbcDialect.find(dialect).isPresent(), "Invalid database dialect: %s", dialect);
      this.dialect = dialect;
      this.host = matcher.group(2);
      this.port = Integer.parseInt(matcher.group(3));
      this.database = matcher.group(4);
    } else {
      //Only extract the dialect
      matcher = JDBC_DIALECT_REGEX.matcher(url);
      if (matcher.find()) {
        String dialect = matcher.group(1);
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
    return (String)map.get("username");
  }

  public String getPassword() {
    return (String)map.get("password");
  }

  public String getDatabase() {
    return this.database;
  }

  public String getUrl() {
    return this.url;
  }
}
