package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.NamePath;
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

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

@Getter
public class QueryRelationalTable extends PhysicalRelationalTable {

  private final LPAnalysis analyzedLP;

  public QueryRelationalTable(Name rootTableId, NamePath tablePath, @NonNull LPAnalysis analyzedLP) {
    super(rootTableId, tablePath,
        analyzedLP.getConvertedRelnode().getType(),
        analyzedLP.getConvertedRelnode().getRelNode().getRowType(),
        analyzedLP.getConvertedRelnode().select.getSourceLength(),
        analyzedLP.getConvertedRelnode().getTimestamp(),
        PrimaryKey.of(analyzedLP.getConvertedRelnode().getPrimaryKey()),
        TableStatistic.of(analyzedLP.getConvertedRelnode().estimateRowCount()));
    Preconditions.checkArgument(analyzedLP.getConvertedRelnode().select.isIdentity(), "We assume an identity select");
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
