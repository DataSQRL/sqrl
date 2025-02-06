package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineFeature.STANDARD_QUERY;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.QueryEngine;

import lombok.Getter;
import lombok.NonNull;

/**
 * Abstract implementation of a relational {@link QueryEngine}.
 */
public abstract class AbstractJDBCQueryEngine extends AbstractJDBCEngine implements QueryEngine {

  @Getter
  final EngineConfig connectorConfig;

  public AbstractJDBCQueryEngine(String name, @NonNull EngineConfig connectorConfig, ConnectorFactoryFactory connectorFactory) {
    super(name, Type.QUERY, STANDARD_QUERY);
    this.connectorConfig = connectorConfig;
  }

}
