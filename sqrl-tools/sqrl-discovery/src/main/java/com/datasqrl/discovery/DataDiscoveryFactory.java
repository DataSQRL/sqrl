package com.datasqrl.discovery;

import static com.datasqrl.config.PipelineFactory.ENGINES_PROPERTY;

import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.datasqrl.engine.database.inmemory.InMemoryDatabaseFactory;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.metadata.InMemoryMetadataStore;
import com.datasqrl.metadata.JdbcMetadataStore;
import com.datasqrl.metadata.MetadataStoreProvider;
import java.util.Optional;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

public class DataDiscoveryFactory {

  public static final String DISCOVERY_KEY = "discovery";

  public static final String DATABASE_ENGINE_KEY = "database";

  public static DataDiscovery fromConfig(@NonNull SqrlConfig config, ErrorCollector errors) {
    PipelineFactory pipelineFactory = new PipelineFactory(config.getSubConfig(ENGINES_PROPERTY));
    SqrlConfig discoveryConfig = config.getSubConfig(DISCOVERY_KEY);
    Optional<String> metadataEngine = discoveryConfig.asString(DATABASE_ENGINE_KEY).validate(
        StringUtils::isNotBlank,"Cannot be empty").getOptional();
    DataDiscovery discovery = new DataDiscovery(errors, pipelineFactory.getStreamEngine(),
        getMetaDataStoreProvider(config.getSubConfig(ENGINES_PROPERTY), metadataEngine),
        pipelineFactory.getEngineConfig());
    return discovery;
  }

  public static DataDiscovery fromPipeline(@NonNull PipelineFactory pipelineFactory, ErrorCollector errors) {
    return new DataDiscovery(errors, pipelineFactory.getStreamEngine(),
        getMetaDataStoreProvider(pipelineFactory.getEngineConfig(), Optional.empty()),
        pipelineFactory.getEngineConfig());
  }

  public static MetadataStoreProvider getMetaDataStoreProvider(SqrlConfig baseEngineConfig, Optional<String> engineIdentifier) {
    for (String engineId : baseEngineConfig.getKeys()) {
      SqrlConfig engineConfig = baseEngineConfig.getSubConfig(engineId);
      EngineFactory engineFactory = EngineFactory.fromConfig(engineConfig);
      if (engineIdentifier.map(id -> id.equalsIgnoreCase(engineId))
          .orElse(engineFactory.getEngineType()== Type.DATABASE)) {
        baseEngineConfig.getErrorCollector().checkFatal(engineFactory instanceof DatabaseEngineFactory,
            "Selected or default engine [%s] for metadata is not a database engine", engineId);
        if (engineFactory instanceof JDBCEngineFactory) {
          JdbcMetadataStore metadataStore = new JdbcMetadataStore();
          return metadataStore.getMetadataStore(engineConfig);
        } else if (engineFactory instanceof InMemoryDatabaseFactory) {
          InMemoryMetadataStore inMemoryMetadataStore = new InMemoryMetadataStore();
          return inMemoryMetadataStore.getMetadataStore(engineConfig);
        } else {
          throw new RuntimeException("Unknown engine type for mata data store");
        }
      }
    }
    throw baseEngineConfig.getErrorCollector().exception("Could not find database engine for metadata");
  }
}
