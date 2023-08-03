package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.SQRLConverter;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;

/**
 * Represents a generic table object in a SQRL script.
 * This could either be a {@link ScriptRelationalTable} which represents standard relational tables
 * or a {@link com.datasqrl.plan.local.generate.TableFunctionBase} which represents a table function.
 */
public interface ScriptTable {

  public String getNameId();

  public Name getTableName();

  List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline, ErrorCollector errors);

  SQRLConverter.Config.ConfigBuilder getBaseConfig();

  void assignStage(ExecutionStage stage);

  RelNode getPlannedRelNode();

  void setPlannedRelNode(RelNode plannedRelNode);

}
