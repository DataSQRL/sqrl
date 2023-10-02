package com.datasqrl.plan.table;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SQRLConverter.Config.ConfigBuilder;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

@Getter
public class QueryRelationalTable extends ScriptRelationalTable {

  private final LPAnalysis analyzedLP;

  public QueryRelationalTable(@NonNull Name rootTableId, @NonNull Name tableName,
      @NonNull LPAnalysis analyzedLP) {
    super(rootTableId, tableName,
        analyzedLP.getConvertedRelnode().getType(),
        analyzedLP.getConvertedRelnode().getRelNode().getRowType(),
        TimestampHolder.Base.ofDerived(analyzedLP.getConvertedRelnode().getTimestamp()),
        analyzedLP.getConvertedRelnode().getPrimaryKey().getLength(),
        TableStatistic.of(analyzedLP.getConvertedRelnode().estimateRowCount()));
    this.analyzedLP = analyzedLP;
  }

  @Override
  public PullupOperator.Container getPullups() {
    return analyzedLP.getConvertedRelnode().getPullups();
  }

  public RelNode getOriginalRelnode() {
    return analyzedLP.getOriginalRelnode();
  }

  @Override
  public List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline, ErrorCollector errors) {
    if (analyzedLP.getConfiguredStages().isEmpty()) return pipeline.getStages();
    return analyzedLP.getConfiguredStages().stream().map(stage -> {
      Optional<ExecutionStage> execStage = pipeline.getStage(stage.getStageName());
      errors.checkFatal(execStage.isPresent(),"Could not find execution stage [%s] "
          + "specified in optimizer hint on table [%s] in pipeline [%s]", stage.getStageName(), this, pipeline);
      return execStage.get();
    }).collect(Collectors.toList());
  }

  @Override
  public ConfigBuilder getBaseConfig() {
    SQRLConverter.Config.ConfigBuilder builder = getAnalyzedLP().getConverterConfig().toBuilder();
    getAssignedStage().ifPresent(stage -> builder.stage(stage));
    return builder;
  }

}
