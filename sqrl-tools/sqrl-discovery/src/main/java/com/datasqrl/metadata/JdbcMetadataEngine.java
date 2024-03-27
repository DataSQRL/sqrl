package com.datasqrl.metadata;

import com.datasqrl.metadata.serializer.KryoProvider;
import com.datasqrl.metadata.serializer.SerializerProvider;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.database.relational.JdbcDataSystemConnector;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

public class JdbcMetadataEngine implements MetadataEngine {

  @Override
  public MetadataStoreProvider getMetadataStore(@NonNull DatabaseEngine databaseEngine) {
    Preconditions.checkArgument(databaseEngine instanceof JDBCEngine, "Not a valid JDBC database: %s", databaseEngine);
    return new StoreProvider(((JDBCEngine) databaseEngine).getConnector());
  }

  @Value
  public static class StoreProvider implements MetadataStoreProvider {

    JdbcDataSystemConnector connection;
    SerializerProvider serializer = new KryoProvider(); //TODO: make configurable

    @Override
    public MetadataStore openStore() {
      return new JdbcMetadataStore(connection, serializer.getSerializer());
    }
  }
}
