package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.rules.SQRLConverter.Config.ConfigBuilder;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.schema.SQRLTable;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

import java.util.List;


@Getter
public class QueryTableFunction extends TableFunctionBase {

  QueryRelationalTable queryTable;

  public QueryTableFunction(Name name, List<FunctionParameter> params, SQRLTable table,
      QueryRelationalTable queryTable) {
    super(name, params, table);
    this.queryTable = queryTable;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return queryTable.getRowType();
  }

  @Override
  public String getNameId() {
    return queryTable.getNameId();
  }

  @Override
  public LPAnalysis getAnalyzedLP() {
    return queryTable.getAnalyzedLP();
  }

  @Override
  public List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline,
      ErrorCollector errors) {
    return queryTable.getSupportedStages(pipeline, errors).stream().filter(ExecutionStage::isRead).collect(
        Collectors.toList());
  }

  @Override
  public ConfigBuilder getBaseConfig() {
    return queryTable.getBaseConfig();
  }

  @Override
  public void assignStage(ExecutionStage stage) {
    queryTable.assignStage(stage);
  }

  @Override
  public Optional<ExecutionStage> getAssignedStage() {
    return queryTable.getAssignedStage();
  }

  @Override
  public RelNode getPlannedRelNode() {
    return queryTable.getPlannedRelNode();
  }

  @Override
  public void setPlannedRelNode(RelNode plannedRelNode) {
    queryTable.setPlannedRelNode(plannedRelNode);
  }


}
