package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SQRLConverter.Config.ConfigBuilder;
import com.datasqrl.plan.table.ScriptRelationalTable;
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

  private Optional<ExecutionStage> assignedStage = Optional.empty();
  @Setter
  private RelNode plannedRelNode = null;


  public AccessTableFunction(Name functionName, List<FunctionParameter> params, RelNode originalRelNode, SQRLTable table) {
    super(functionName, params, originalRelNode, table);
  }

  @Override
  public Type getElementType(List<Object> list) {
    return Object.class;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return params;
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
