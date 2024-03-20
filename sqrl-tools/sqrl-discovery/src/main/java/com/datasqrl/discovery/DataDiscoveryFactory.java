package com.datasqrl.discovery;

import static com.datasqrl.config.PipelineFactory.ENGINES_PROPERTY;

import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.discovery.system.DataSystemDiscovery;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.flink.AbstractFlinkStreamEngine;
import com.datasqrl.discovery.flink.FlinkMonitoringJobFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.metadata.JdbcMetadataEngine;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Optional;
import lombok.NonNull;

public class DataDiscoveryFactory {

  public static final String DISCOVERY_KEY = "discovery";

  public static DataDiscovery fromConfig(@NonNull SqrlConfig config, ErrorCollector errors) {
    SqrlConfig enginesConfig = config.getSubConfig(ENGINES_PROPERTY);
    PipelineFactory pipelineFactory = new PipelineFactory(enginesConfig);
    SqrlConfig discoveryConfig = config.getSubConfig(DISCOVERY_KEY);
    DataDiscovery discovery = new DataDiscovery(
        DataDiscoveryConfig.of(discoveryConfig, errors),
        getJobFactory(pipelineFactory.getStreamEngine()),
        getMetaDataStoreProvider(pipelineFactory.getDatabaseEngine()));
    return discovery;
  }

  public static MetadataStoreProvider getMetaDataStoreProvider(@NonNull DatabaseEngine databaseEngine) {
    if (databaseEngine instanceof JDBCEngine) {
      JdbcMetadataEngine metadataStore = new JdbcMetadataEngine();
      return metadataStore.getMetadataStore(databaseEngine);
    } else {
      throw new RuntimeException("Unsupported engine type for meta data store: " + databaseEngine);
    }
  }


  public static MonitoringJobFactory getJobFactory(@NonNull StreamEngine streamEngine) {
    if (streamEngine instanceof AbstractFlinkStreamEngine) {
      return new FlinkMonitoringJobFactory(((AbstractFlinkStreamEngine) streamEngine));
    } else {
      throw new RuntimeException("Unsupported engine type for monitoring: " + streamEngine);
    }
  }

  public static Optional<String> inferDataSystemFromArgument(@NonNull String systemConfig) {
    return ServiceLoaderDiscovery.getAll(DataSystemDiscovery.class).stream().filter(system -> system.matchesArgument(systemConfig))
        .findFirst().map(DataSystemDiscovery::getType);
  }
}
