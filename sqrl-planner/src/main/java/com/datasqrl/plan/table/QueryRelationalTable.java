package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.hints.OptimizerHint;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

public class QueryRelationalTable extends PhysicalRelationalTable {

  private final LPAnalysis analyzedLP;
  @Getter
  private final Optional<PhysicalRelationalTable> streamRoot;

  public QueryRelationalTable(Name rootTableId, NamePath tablePath, @NonNull LPAnalysis analyzedLP) {
    super(rootTableId, tablePath,
        analyzedLP.getConvertedRelnode().getType(),
        analyzedLP.getConvertedRelnode().getRelNode().getRowType(),
        analyzedLP.getConvertedRelnode().select.getSourceLength(),
        analyzedLP.getConvertedRelnode().getTimestamp(),
        PrimaryKey.of(analyzedLP.getConvertedRelnode().getPrimaryKey()),
        analyzedLP.getConvertedRelnode().getPullups(),
        TableStatistic.of(analyzedLP.getConvertedRelnode().estimateRowCount()));
    Preconditions.checkArgument(analyzedLP.getConvertedRelnode().select.isIdentity(), "We assume an identity select");
    this.analyzedLP = analyzedLP;
    this.streamRoot = analyzedLP.getConvertedRelnode().getStreamRoot();
  }

  public List<OptimizerHint> getOptimizerHints() {
    return analyzedLP.getSqrlHints();
  }

  public RelNode getOriginalRelnode() {
    return analyzedLP.getOriginalRelnode();
  }

  @Override
  public List<ExecutionStage> getSupportedStages(ExecutionPipeline pipeline, ErrorCollector errors) {
    if (analyzedLP.getConfiguredStages().isEmpty()) return pipeline.getStages();
    return analyzedLP.getConfiguredStages().stream().map(stage -> {
      Optional<ExecutionStage> execStage = pipeline.getStage(stage.getStageName())
          .or(()->pipeline.getStageByType(stage.getStageName()));
      errors.checkFatal(execStage.isPresent(),"Could not find execution stage [%s] "
          + "specified in optimizer hint on table [%s] in pipeline [%s]", stage.getStageName(), this, pipeline);
      return execStage.get();
    }).collect(Collectors.toList());
  }

  @Override
  public SqrlConverterConfig.SqrlConverterConfigBuilder getBaseConfig() {
    SqrlConverterConfig.SqrlConverterConfigBuilder builder = analyzedLP.getConverterConfig().toBuilder();
    getAssignedStage().ifPresent(builder::stage);
    return builder;
  }

}
