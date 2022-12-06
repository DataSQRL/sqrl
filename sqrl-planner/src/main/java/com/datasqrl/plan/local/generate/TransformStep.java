package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import java.util.Map;
import java.util.function.Function;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

@Value
public class TransformStep {
  Function<Analysis, SqlShuttle> transform;
  Function<SqlNode, Analysis> analyzer;
}
