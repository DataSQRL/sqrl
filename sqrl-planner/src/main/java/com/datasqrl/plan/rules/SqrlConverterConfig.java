package com.datasqrl.plan.rules;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import java.util.List;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@AllArgsConstructor
@Getter
public class SqrlConverterConfig {

  ExecutionStage stage;

  @Builder.Default
  Consumer<PhysicalRelationalTable> sourceTableConsumer = (t) -> {
  };
  @Builder.Default
  int slideWindowPanes = SQRLConverter.DEFAULT_SLIDING_WINDOW_PANES;

  @Builder.Default
  boolean setOriginalFieldnames = false;

  @Builder.Default
  List<String> fieldNames = null;

  @Builder.Default
  boolean inlinePullups = false;

  public SqrlConverterConfig withStage(ExecutionStage stage) {
    return toBuilder().stage(stage).build();
  }

  public ExecutionAnalysis getExecAnalysis() {
    return ExecutionAnalysis.of(getStage());
  }

}
