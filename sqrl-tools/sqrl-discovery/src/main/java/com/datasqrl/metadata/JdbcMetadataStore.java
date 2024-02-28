package com.datasqrl.metadata;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.serializer.KryoProvider;
import com.datasqrl.config.serializer.SerializerProvider;
import com.datasqrl.engine.database.relational.metadata.JDBCMetadataStore;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnectorFactory;
import lombok.NonNull;
import lombok.Value;

public class JdbcMetadataStore implements MetadataEngine {

  @Override
  public MetadataStoreProvider getMetadataStore(@NonNull SqrlConfig config) {
    return new StoreProvider(new JdbcDataSystemConnectorFactory().getConnector(config));
  }

  @Value
  public static class StoreProvider implements MetadataStoreProvider {

    JdbcDataSystemConnector connection;
    SerializerProvider serializer = new KryoProvider(); //TODO: make configurable

    @Override
    public MetadataStore openStore() {
      return new JDBCMetadataStore(connection, serializer.getSerializer());
    }
  }
}
