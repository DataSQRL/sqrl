package com.datasqrl.metadata;

import com.datasqrl.metadata.serializer.KryoProvider;
import com.datasqrl.metadata.serializer.SerializerProvider;
import com.datasqrl.engine.database.DatabaseEngine;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

public class JdbcMetadataEngine implements MetadataEngine {

  @Override
  public MetadataStoreProvider getMetadataStore(@NonNull DatabaseEngine databaseEngine) {
//    Preconditions.checkArgument(databaseEngine instanceof JDBCEngine, "Not a valid JDBC database: %s", databaseEngine);
//    return new StoreProvider(((JDBCEngine) databaseEngine).getConnector());
    throw new RuntimeException("TODO");
  }

  @AllArgsConstructor
  @Getter
  public static class StoreProvider implements MetadataStoreProvider {

    final SerializerProvider serializer = new KryoProvider(); //TODO: make configurable

    @Override
    public MetadataStore openStore() {
      throw new RuntimeException("todo");
//      return new JdbcMetadataStore(connection, serializer.getSerializer());
    }
  }
}
