package com.datasqrl.engine.database.relational;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.serializer.KryoProvider;
import com.datasqrl.config.serializer.SerializerProvider;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.datasqrl.engine.database.relational.metadata.JDBCMetadataStore;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnectorFactory;
import com.datasqrl.metadata.MetadataStore;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.google.auto.service.AutoService;
import lombok.NonNull;
import lombok.Value;

@AutoService(EngineFactory.class)
public class JDBCEngineFactory implements DatabaseEngineFactory {
  public static final String ENGINE_NAME = "jdbc";

  public static final String GENERATE_QUERIES_KEY = "generate";
  public static final boolean GENERATE_QUERIES_DEFAULT = false;

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public MetadataStoreProvider getMetadataStore(@NonNull SqrlConfig config) {
    return new StoreProvider(getConnector(config));
  }

  @Override
  public JDBCEngine initialize(@NonNull SqrlConfig config) {
    return new JDBCEngine(getConnector(config),
        config.asBool(GENERATE_QUERIES_KEY).withDefault(GENERATE_QUERIES_DEFAULT).get());
  }

  private JdbcDataSystemConnector getConnector(@NonNull SqrlConfig config) {
    return new JdbcDataSystemConnectorFactory().getConnector(config);
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
