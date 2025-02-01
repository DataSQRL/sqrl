package com.datasqrl.plan.global;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.EngineType;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.FunctionParameter;

public interface DatabaseQuery {

  IdentifiedQuery getQueryId();
  RelNode getRelNode(ExecutionStage stage, SQRLConverter sqrlConverter, ErrorCollector errors);
  default String getName() {
    return getQueryId().getNameId();
  }

  static DatabaseQuery.Instance of(AbstractRelationalTable table) {
    Preconditions.checkArgument(table instanceof PhysicalRelationalTable, "Expected physical table");
    PhysicalRelationalTable vTable = (PhysicalRelationalTable)table;
    Preconditions.checkArgument(vTable.isRoot());
    ExecutionStage stage = vTable.getAssignedStage().get();
    //TODO: We don't yet support server queries directly against materialized tables. Need a database stage in between.
    Preconditions.checkArgument(stage.getEngine().getType()== EngineType.DATABASE, "We do not yet support queries directly against stream");
    return new Instance(vTable.getNameId(), vTable.getTableName(), List.of(), vTable.getPlannedRelNode(), stage);
  }

  static DatabaseQuery.Instance of(QueryTableFunction function) {
    QueryRelationalTable queryTable = function.getQueryTable();
    ExecutionStage assignedStage = queryTable.getAssignedStage().get();
    Preconditions.checkArgument(assignedStage.getEngine().getType()== EngineType.DATABASE);
    return new Instance(queryTable.getNameId(), queryTable.getTableName(), function.getParameters(), queryTable.getPlannedRelNode(), assignedStage);
  }


  @Value
  class Instance implements DatabaseQuery, IdentifiedQuery {

    String nameId;
    Name tableName;
    List<FunctionParameter> paramters;
    RelNode plannedRelNode;
    ExecutionStage assignedStage;

    @Override
    public String getNameId() {
      return nameId;
    }

    @Override
    public Optional<Name> getViewName() {
      if (!paramters.isEmpty()) return Optional.empty();
      return Optional.of(tableName);
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
