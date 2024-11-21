package com.datasqrl.plan.rules;

import com.datasqrl.plan.hints.OptimizerHint;
import com.datasqrl.plan.hints.PipelineStageHint;
import java.util.List;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNode;

@Value
public class LPAnalysis {

  @NonNull RelNode originalRelNode;

  @NonNull SqlNode originalSqlNode;

  @NonNull AnnotatedLP convertedRelnode;

  @NonNull List<PipelineStageHint> configuredStages;

  @NonNull SqrlConverterConfig converterConfig;

  @NonNull List<OptimizerHint> sqrlHints;

  @NonNull List<SqlHint> sqlHints;

}
