package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.iceberg.IcebergDataTypeMapper;
import com.datasqrl.engine.database.DatabasePhysicalPlanOld;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.NonNull;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

public class IcebergEngine extends AbstractJDBCTableFormatEngine {

  @Inject
  public IcebergEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(IcebergEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfig(IcebergEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(IcebergEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  public boolean supportsQueryEngine(QueryEngine engine) {
    return engine instanceof SnowflakeEngine || engine instanceof DuckDBEngine;
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Iceberg;
  }

  @Override
  protected JdbcStatementFactory getStatementFactory() {
    return new IcebergStatementFactory();
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return new IcebergDataTypeMapper();
  }

  @Override
  @Deprecated
  public DatabasePhysicalPlanOld plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {

    List<SqlDDLStatement> sinkDDL;
    //TODO: Eventually we want to create the DDL statement for Iceberg to register in the catalog independent
    //of the registered query engines. For now, we use the DDL from one of the query engines
//    DatabasePhysicalPlan sinkPlan = super.plan(plan, inputs, pipeline, stagePlans, framework, errorCollector);
//    sinkPlan.removeIndexDdl();
//    sinkDDL = sinkPlan.getDdl();


    //The plan for reading by each query engine
    LinkedHashMap<String, DatabasePhysicalPlanOld> queryEnginePlans = new LinkedHashMap<>();
    queryEngines.forEach((name, queryEngine) -> queryEnginePlans.put(name,
        queryEngine.plan(connectorFactory, engineConfig, plan, inputs, pipeline, stagePlans, framework, errorCollector)));

    //We pick the first DDL from the engines
    sinkDDL = queryEnginePlans.values().stream().map(DatabasePhysicalPlanOld::getDdl).findFirst()
        .orElse(List.of());
    
    //Uncomment for debug
//    StreamUtil.filterByClass(inputs,
//        EngineSink.class).forEach(s -> {
//          String tableId = s.getNameId();
//          Optional<IndexDefinition> optIndex =  dbPlan.getIndexDefinitions().stream().filter(i -> i.getTableId().equals(tableId)).findFirst();
//          if (optIndex.isPresent()) {
//            IndexDefinition mainIndex = optIndex.get();
//            System.out.println("Table: " + tableId);
//            System.out.println("Partition columns: " + String.join(", ",  mainIndex.getColumnNames().subList(0, mainIndex.getPartitionOffset())));
//            System.out.println("Sort columns: " + String.join(", ",  mainIndex.getColumnNames().subList(mainIndex.getPartitionOffset(), mainIndex.getColumns().size())));
//          } else {
//            System.out.println("No partition on table: " + tableId);
//          }
//    });

    //nothing needs to be created for aws-glue
    return new IcebergPlan(sinkDDL, queryEnginePlans);
  }

  @Override
  protected String createView(SqlIdentifier viewNameIdentifier, SqlParserPos pos,
      SqlNodeList columnList, SqlNode viewSqlNode) {
    throw new UnsupportedOperationException("Should not be called since we overwrite #plan");
  }



}
