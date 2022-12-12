package com.datasqrl.engine.stream.flink;

import com.datasqrl.Sink;
import com.datasqrl.config.provider.DatabaseConnectionProvider;
import com.datasqrl.config.provider.JDBCConnectionProvider;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkPipelineUtils;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.global.OptimizedDAG.WriteSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

public class DBSink implements Sink<OptimizedDAG.DatabaseSink> {

  @Override
  public TableDescriptor create(OptimizedDAG.DatabaseSink dbSink, Schema tblSchema) {
    DatabaseEngine dbEngine = (DatabaseEngine) dbSink.getDatabaseStage().getEngine();
    DatabaseConnectionProvider dbConnection = dbEngine.getConnectionProvider();

    tblSchema = FlinkPipelineUtils.addPrimaryKey(tblSchema, dbSink);
    if (dbConnection instanceof JDBCConnectionProvider) {
      JDBCConnectionProvider jdbcConfiguration = (JDBCConnectionProvider) dbConnection;
      return TableDescriptor.forConnector("jdbc")
          .schema(tblSchema)
          .option("url", jdbcConfiguration.getDbURL())
          .option("table-name", dbSink.getName())
          .option("username", jdbcConfiguration.getUser())
          .option("password", jdbcConfiguration.getPassword())
          .build();
    }
    throw new RuntimeException();
  }
}
