/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnectorConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.engine.database.relational.metadata.JDBCMetadataStore;
import com.datasqrl.metadata.MetadataStore;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.config.serializer.KryoProvider;
import com.datasqrl.config.serializer.SerializerProvider;
import com.datasqrl.util.ConfigurationUtil;
import com.datasqrl.engine.database.DatabaseEngineConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.base.Strings;
import lombok.*;

import java.util.regex.Pattern;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JDBCEngineConfiguration implements DatabaseEngineConfiguration {

  public static final String ENGINE_NAME = "jdbc";

  @JsonUnwrapped
  JdbcDataSystemConnectorConfig config;

  public static Pattern validDBName = Pattern.compile("^[a-z][_a-z0-9$]{2,}$");

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  private boolean validate(@NonNull ErrorCollector errors) {
    ConfigurationUtil.javaxValidate(this, errors);
    if (Strings.isNullOrEmpty(config.getDatabase()) || !validDBName.matcher(config.getDatabase()).matches()) {
      errors.fatal("Invalid database name: %s", config.getDatabase());
      return false;
    }
    return true;
  }

  @Override
  public JDBCEngine initialize(@NonNull ErrorCollector errors) {
    if (validate(errors)) {
      return new JDBCEngine(this);
    } else {
      return null;
    }
  }

  @Override
  @JsonIgnore
  public MetadataStoreProvider getMetadataStore() {
    return new StoreProvider(getConfig());
  }

  @Value
  public static class StoreProvider implements MetadataStoreProvider {

    JdbcDataSystemConnectorConfig connection;
    SerializerProvider serializer = new KryoProvider(); //TODO: make configurable

    @Override
    public MetadataStore openStore() {
      return new JDBCMetadataStore(connection, serializer.getSerializer());
    }

  }

}
