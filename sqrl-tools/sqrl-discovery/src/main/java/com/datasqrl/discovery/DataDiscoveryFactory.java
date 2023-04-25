package com.datasqrl.discovery;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

public class DataDiscoveryFactory {

  public static final String DISCOVERY_KEY = "discovery";

  public static final String DATABASE_ENGINE_KEY = "database";

  public static DataDiscovery fromConfig(@NonNull SqrlConfig config, ErrorCollector errors) {
    PipelineFactory pipelineFactory = PipelineFactory.fromRootConfig(config);
    SqrlConfig discoveryConfig = config.getSubConfig(DISCOVERY_KEY);
    Optional<String> metadataEngine = discoveryConfig.asString(DATABASE_ENGINE_KEY).validate(
        StringUtils::isNotBlank,"Cannot be empty").getOptional();
    DataDiscovery discovery = new DataDiscovery(errors, pipelineFactory.getStreamEngine(),
        pipelineFactory.getMetaDataStoreProvider(metadataEngine));
    return discovery;
  }

  public static DataDiscovery fromPipeline(@NonNull PipelineFactory pipelineFactory, ErrorCollector errors) {
    return new DataDiscovery(errors, pipelineFactory.getStreamEngine(),
        pipelineFactory.getMetaDataStoreProvider(Optional.empty()));
  }

}
