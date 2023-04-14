package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

public class SqlTransformer {

  private final Function<SqlNode, Analysis> analyzer;
  private final List<Function<Analysis, SqlShuttle>> shuttles;

  public SqlTransformer(Function<SqlNode, Analysis> analyzer,
      Function<Analysis, SqlShuttle>... shuttle) {
    this.analyzer = analyzer;
    this.shuttles = Arrays.asList(shuttle);
  }

  public SqlNode transform(SqlNode node) {
    Analysis currentAnalysis = null;
    for (Function<Analysis, SqlShuttle> transform : shuttles) {
      SqlShuttle shuttle = transform.apply(currentAnalysis);
      node = node.accept(shuttle);
      currentAnalysis = analyzer.apply(node);
    }
    return node;
  }
}
