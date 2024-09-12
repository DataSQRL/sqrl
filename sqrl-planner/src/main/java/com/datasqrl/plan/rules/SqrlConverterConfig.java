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

  /**
   * Overwrite the field names with the ones from the relnode
   */
  @Builder.Default
  boolean setOriginalFieldnames = false;

  /**
   * Overwrite the field names with these given ones
   */
  @Builder.Default
  List<String> fieldNames = null;

  /**
   * Whether to inline pullups instead of pulling them up
   */
  @Builder.Default
  boolean inlinePullups = false;

  /**
   * Overwrite the primary key names with these given ones
   */
  @Builder.Default
  List<String> primaryKeyNames = null;

  /**
   * Whether to add a static literal as primary key when the table doesn't have one.
   * This is needed to write tables to the database
   */
  @Builder.Default
  boolean addDefaultPrimaryKey = false;

  public SqrlConverterConfig withStage(ExecutionStage stage) {
    return toBuilder().stage(stage).build();
  }

  public ExecutionAnalysis getExecAnalysis() {
    return ExecutionAnalysis.of(getStage());
  }

}
