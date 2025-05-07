package com.datasqrl.engine.export;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import com.google.inject.Inject;

public class PrintEngine implements ExportEngine {

  private final ConnectorConf connectorConf;

  @Inject
  public PrintEngine(ConnectorFactoryFactory connectorFactory) {
    this.connectorConf = connectorFactory.getConfig(PrintEngineFactory.NAME);
  }

  @Override
  public EngineCreateTable createTable(ExecutionStage stage, String originalTableName,
      FlinkTableBuilder tableBuilder, RelDataType relDataType, Optional<TableAnalysis> tableAnalysis) {
    tableBuilder.setConnectorOptions(connectorConf.toMapWithSubstitution(
        Context.builder()
            .tableName(tableBuilder.getTableName())
            .origTableName(originalTableName)
            .build()));
    return EngineCreateTable.NONE;
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return DataTypeMapping.NONE;
  }

  @Override
  public boolean supports(EngineFeature capability) {
    return false;
  }

  @Override
  public String getName() {
    return PrintEngineFactory.NAME;
  }

  @Override
  public EngineType getType() {
    return EngineType.EXPORT;
  }
}
