package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.plan.table.PullupOperator.Container;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Represents a generic table object in a SQRL script.
 * This could either be a {@link PhysicalRelationalTable} which represents standard relational tables.
 */
public interface PhysicalTable {

  String getNameId();

  Name getTableName();

  TableType getType();

  List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline, ErrorCollector errors);

  SqrlConverterConfig.SqrlConverterConfigBuilder getBaseConfig();

  void assignStage(ExecutionStage stage);

  Optional<ExecutionStage> getAssignedStage();

  RelNode getPlannedRelNode();

  Timestamps getTimestamp();

  void setPlannedRelNode(SQRLConverter.TablePlan plan);

}
