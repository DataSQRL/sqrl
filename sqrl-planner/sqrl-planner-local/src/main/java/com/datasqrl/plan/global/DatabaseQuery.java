package com.datasqrl.plan.global;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.local.generate.AccessTableFunction;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.local.generate.TableFunctionBase;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

public interface DatabaseQuery {

  IdentifiedQuery getQueryId();
  RelNode getRelNode(ExecutionStage stage, SQRLConverter sqrlConverter, ErrorCollector errors);
  default String getName() {
    return getQueryId().getNameId();
  }

  static DatabaseQuery.Instance of(AbstractRelationalTable table) {
    Preconditions.checkArgument(table instanceof VirtualRelationalTable.Root, "Expected root virtual table");
    VirtualRelationalTable.Root vTable = (VirtualRelationalTable.Root)table;
    Preconditions.checkArgument(vTable.isRoot());
    ExecutionStage stage = vTable.getRoot().getBase().getAssignedStage().get();
    //TODO: We don't yet support server queries directly against materialized tables. Need a database stage in between.
    Preconditions.checkArgument(stage.isRead(), "We do not yet support queries directly against stream");
    return new Instance(vTable.getNameId(), vTable.getRoot().getBase().getPlannedRelNode(), stage);
  }

  static DatabaseQuery.Instance of(TableFunctionBase function) {
    ExecutionStage assignedStage;
    if (function instanceof AccessTableFunction) {
      AccessTableFunction accessFct = (AccessTableFunction) function;
      assignedStage = accessFct.getAssignedStage().get();
    } else {
      QueryTableFunction computeFct = (QueryTableFunction) function;
      assignedStage = computeFct.getQueryTable().getAssignedStage().get();
    }
    return new Instance(function.getNameId(), function.getPlannedRelNode(), assignedStage);
  }


  @Value
  class Instance implements DatabaseQuery, IdentifiedQuery {

    String nameId;
    RelNode plannedRelNode;
    ExecutionStage assignedStage;

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

}
