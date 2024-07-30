package com.datasqrl.plan.global;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.plan.rules.SQRLConverter;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class DatabaseQueryImpl implements DatabaseQuery, IdentifiedQuery {

  String nameId;
  RelNode plannedRelNode;
  ExecutionStage assignedStage;
  NamePath namePath;

  @Override
  public String getNameId() {
    return nameId;
  }

  @Override
  public IdentifiedQuery getQueryId() {
    return this;
  }

  @Override
  public RelNode getRelNode(ExecutionStage stage, SQRLConverter sqrlConverter,
      ErrorCollector errors) {
    Preconditions.checkArgument(assignedStage.equals(stage),
        "Mismatch in stage: %s (assigned) vs %s (requested)", assignedStage, stage);
    return plannedRelNode;
  }
}