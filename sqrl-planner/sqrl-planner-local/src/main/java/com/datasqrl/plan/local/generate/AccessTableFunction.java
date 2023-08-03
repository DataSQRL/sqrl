package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SQRLConverter.Config.ConfigBuilder;
import com.datasqrl.plan.table.SortOrder;
import com.datasqrl.schema.SQRLTable;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

import java.lang.reflect.Type;
import java.util.List;

@Getter
public class AccessTableFunction extends TableFunctionBase {

  private final LPAnalysis analyzedLP;

  private Optional<ExecutionStage> assignedStage = Optional.empty();
  @Setter
  private RelNode plannedRelNode = null;

  public AccessTableFunction(Name functionName, List<FunctionParameter> params, LPAnalysis analyzedLP, SQRLTable table) {
    super(functionName, params, table);
    this.analyzedLP = analyzedLP;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return analyzedLP.getConvertedRelnode().getRelNode().getRowType();
  }

  @Override
  public List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline,
      ErrorCollector errors) {
    return pipeline.getReadStages();
  }

  @Override
  public ConfigBuilder getBaseConfig() {
    SQRLConverter.Config.ConfigBuilder builder = SQRLConverter.Config.builder();
    assignedStage.ifPresent(stage -> builder.stage(stage));
    return builder;
  }

  @Override
  public void assignStage(ExecutionStage stage) {
    assignedStage = Optional.of(stage);
  }


}
