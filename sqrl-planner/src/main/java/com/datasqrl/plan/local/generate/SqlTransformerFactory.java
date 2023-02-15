package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.local.transpile.AddContextTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.plan.local.transpile.*;
import java.util.function.Function;
import org.apache.calcite.sql.SqlNode;

public class SqlTransformerFactory {
  public static SqlTransformer create(Function<SqlNode, Analysis> analyzer,
      boolean hasContext) {
    return new SqlTransformer(analyzer,
        (analysis) -> new AddContextTable(hasContext),
        QualifyIdentifiers::new,
        FlattenFieldPaths::new,
        FlattenTablePaths::new,
        ReplaceWithVirtualTable::new,
        AllowMixedFieldUnions::new,
        MapSystemFunctions::new
    );
  }
}