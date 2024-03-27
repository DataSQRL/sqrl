/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.Constraints.MinLength;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.tables.FlinkConnectorFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import lombok.NonNull;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JdbcDataSystemConnector implements Serializable {

  @MinLength(min = 3)
  String url;
  @MinLength(min = 2)
  String dialect;
  @Default
  String database = null;
  @Default
  String host = null;
  @Default
  Integer port = null;
  @Default
  String user = null;
  @Default
  String password = null;
  @Default @MinLength(min = 3)
  String driver = null;

  public JdbcDialect getDialect() {
    return JdbcDialect.find(dialect).orElseThrow();
  }

  public static final Pattern JDBC_URL_REGEX = Pattern.compile("^jdbc:(.*?):\\/\\/([^/:]*)(?::(\\d+))?\\/([^/:?]*)(.*)$");
  public static final Pattern JDBC_DIALECT_REGEX = Pattern.compile("^jdbc:(.*?):(.*)$");

  public static JdbcDataSystemConnector fromFlinkConnector(@NonNull SqrlConfig connectorConfig) {
//    Preconditions.checkArgument(connectorConfig.asString(FlinkConnectorFactory.CONNECTOR_KEY).get().equals("jdbc"));
    JdbcDataSystemConnectorBuilder builder = builder();
    String url = connectorConfig.asString("url").get();

    //todo use: Properties properties = Driver.parseURL(connector.getUrl(), null);
    builder.url(url);
    Matcher matcher = JDBC_URL_REGEX.matcher(url);
    if (matcher.find()) {
      String dialect = matcher.group(1);
      connectorConfig.getErrorCollector().checkFatal(JdbcDialect.find(dialect).isPresent(), "Invalid database dialect: %s", dialect);
      builder.dialect(dialect);
      builder.host(matcher.group(2));
      builder.port(Integer.parseInt(matcher.group(3)));
      builder.database(matcher.group(4));
    } else {
      //Only extract the dialect
      matcher = JDBC_DIALECT_REGEX.matcher(url);
      if (matcher.find()) {
        String dialect = matcher.group(1);
        connectorConfig.getErrorCollector().checkFatal(JdbcDialect.find(dialect).isPresent(), "Invalid database dialect: %s", dialect);
        builder.dialect(dialect);
      } else {
        throw connectorConfig.getErrorCollector().exception("Invalid database URL: %s", url);
      }
    }
    connectorConfig.asString("username").getOptional().ifPresent(builder::user);
    connectorConfig.asString("password").getOptional().ifPresent(builder::password);
    connectorConfig.asString("driver").getOptional().ifPresent(builder::driver);
    return builder.build();
  }

  public SqrlConfig toFlinkConnector() {
    SqrlConfig connectorConfig = SqrlConfig.createCurrentVersion();
    connectorConfig.setProperty(FlinkConnectorFactory.CONNECTOR_KEY, "jdbc-sqrl");
    connectorConfig.setProperty("url", url);
    Optional.ofNullable(driver).ifPresent(v->connectorConfig.setProperty("driver", v));
    Optional.ofNullable(user).ifPresent(v->connectorConfig.setProperty("username", v));
    Optional.ofNullable(password).ifPresent(v->connectorConfig.setProperty("password", v));
    return connectorConfig;
  }



}
