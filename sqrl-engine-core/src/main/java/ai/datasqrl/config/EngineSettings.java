package ai.datasqrl.config;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.metadata.MetadataConfiguration;
import ai.datasqrl.metadata.MetadataStoreProvider;
import ai.datasqrl.physical.EngineConfiguration;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.database.DatabaseEngine;
import ai.datasqrl.physical.database.DatabaseEngineConfiguration;
import ai.datasqrl.physical.pipeline.EnginePipeline;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.physical.stream.StreamEngine;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;

@Builder
@AllArgsConstructor
@NoArgsConstructor
/**
 * TODO: need to add proper error handling
 */
public class EngineSettings {

  @NonNull
  List<EngineConfiguration> engines;
  @NonNull
  MetadataConfiguration metadata;

  public MetadataStoreProvider getMetadataStoreProvider() {
    DatabaseEngineConfiguration engConfig;
    if (Strings.isNullOrEmpty(metadata.getEngineType())) {
      engConfig = findDatabase();
    } else {
      engConfig = StreamUtil.filterByClass(engines,DatabaseEngineConfiguration.class)
              .filter(eng -> eng.getEngineName().equalsIgnoreCase(metadata.getEngineType()))
              .findAny().orElseThrow(() -> new IllegalArgumentException("Could not find engine with type: " + metadata.getEngineType()));
    }
    return engConfig.getMetadataStore();
  }

  private DatabaseEngineConfiguration findDatabase() {
    return StreamUtil.filterByClass(engines,DatabaseEngineConfiguration.class)
            .findFirst().orElseThrow(() -> new IllegalArgumentException("Need to configure a database engine"));
  }

  private EngineConfiguration findStream() {
    return engines.stream().filter(conf -> conf.getEngineType()== ExecutionEngine.Type.STREAM)
            .findFirst().orElseThrow(() -> new IllegalArgumentException("Need to configure a stream engine"));
  }

  public DatabaseConnectionProvider getDatabase(ErrorCollector errors) {
    DatabaseEngine db = findDatabase().initialize(errors);
    if (db!=null) return db.getConnectionProvider();
    else return null;
  }

  public JDBCConnectionProvider getJDBC(ErrorCollector errors) {
    return (JDBCConnectionProvider) getDatabase(errors);
  }

  public StreamEngine getStream(ErrorCollector errors) {
    return (StreamEngine) findStream().initialize(errors);
  }

  public ExecutionPipeline getPipeline(ErrorCollector errors) {
    return new EnginePipeline(findDatabase().initialize(errors),getStream(errors));
  }





}
