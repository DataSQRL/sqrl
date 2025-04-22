package com.datasqrl.plan.rules;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlHint;

import com.datasqrl.plan.hints.OptimizerHint;
import com.datasqrl.plan.hints.PipelineStageHint;

import lombok.NonNull;
import lombok.Value;

@Value
public class LPAnalysis {

  @NonNull RelNode originalRelnode;

  @NonNull AnnotatedLP convertedRelnode;

  @NonNull List<PipelineStageHint> configuredStages;

  @NonNull SqrlConverterConfig converterConfig;

  @NonNull List<OptimizerHint> sqrlHints;

  @NonNull List<SqlHint> sqlHints;

}
