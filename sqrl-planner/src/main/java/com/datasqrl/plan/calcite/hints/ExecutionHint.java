package com.datasqrl.plan.calcite.hints;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import lombok.Value;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNodeList;

import java.util.Map;
import java.util.Optional;

@Value
public class ExecutionHint {

  public static final Map<String, ExecutionEngine.Type> NAME2TYPE = ImmutableMap.of(
      "exec_db", ExecutionEngine.Type.DATABASE,
      "exec_stream", ExecutionEngine.Type.STREAM,
      "exec_server", ExecutionEngine.Type.SERVER
  );

  private final ExecutionEngine.Type execType;

  public static Optional<ExecutionHint> fromSqlHint(Optional<SqlNodeList> hints) {
    if (hints.isPresent()) {
      for (SqlHint hint : Iterables.filter(hints.get().getList(), SqlHint.class)) {
        Optional<ExecutionEngine.Type> execType = NAME2TYPE.entrySet().stream()
            .filter(e -> hint.getName().equalsIgnoreCase(e.getKey()))
            .map(Map.Entry::getValue).findFirst();
        if (execType.isPresent()) {
          return Optional.of(new ExecutionHint(execType.get()));
        }
      }
    }
    return Optional.empty();
  }

  public SQRLLogicalPlanConverter.Config.ConfigBuilder getConfig(ExecutionPipeline pipeline,
      SQRLLogicalPlanConverter.Config.ConfigBuilder configBuilder) {
    boolean allowStateChange = false;
    ExecutionEngine.Type startType;
    switch (execType) {
      case STREAM:
        startType = ExecutionEngine.Type.STREAM;
        break;
      case DATABASE:
        startType = ExecutionEngine.Type.DATABASE;
        break;
      case SERVER:
        startType = ExecutionEngine.Type.DATABASE;
        allowStateChange = true;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported engine type: " + execType);
    }
    Optional<ExecutionStage> startStage = pipeline.getStage(startType);
    Preconditions.checkArgument(startStage.isPresent(),
        "Execution type [%s] is not supported by pipeline", execType);
    return configBuilder.startStage(startStage.get()).allowStageChange(allowStateChange);
  }

}
