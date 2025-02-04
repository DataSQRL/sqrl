package com.datasqrl.v2.dag.plan;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.v2.tables.SqrlTableFunction;
import java.util.List;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.rel.RelNode;

/**
 * During DAG planning, we use this stage plan to keep track of all
 * exports and imports to this execution stage plus any queries we
 * run against it.
 */
@Value
@SuperBuilder
public class MaterializationStagePlan {

  ExecutionStage stage;
  /**
   * All the sinks for that stage which are tables we export to in Flink
   */
  @Singular
  List<EngineCreateTable> tables;
  /**
   * All the queries that this stage executes against the data, only applies to databse/log stages
   */
  @Singular
  List<Query> queries;

  /**
   * All the mutations we write to this stage, only applies to logs
   */
  @Singular
  List<EngineCreateTable> mutations;

  /**
   * Passed through since the engines might need it for query manipulation
   */
  Utils utils;

  @Value
  public static class Query {
    SqrlTableFunction function;
    RelNode relNode;
    ErrorCollector errors;
  }

  @Value
  public static class Utils {
    SqrlRexUtil rexUtil;
  }


}
