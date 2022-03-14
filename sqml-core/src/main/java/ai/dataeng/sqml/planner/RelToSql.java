package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.planner.operator2.SqrlTableScan;
import com.google.common.collect.ImmutableList;
import java.util.function.UnaryOperator;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class RelToSql {

  public static String convertToSql(RelNode optimizedNode) {
//    optimizedNode = optimizedNode.accept(new RelShuttleImpl(){
//      @Override
//      public RelNode visit(TableScan scan) {
//        SqrlCalciteTable tbl = scan.getTable().unwrap(SqrlCalciteTable.class);
//
//        RelOptTableImpl t = RelOptTableImpl.create(scan.getTable().getRelOptSchema(), scan.getTable().getRowType(),
//            tbl, ImmutableList.of(tbl.getSqrlTable().getPath().toString()));
//
//        return SqrlTableScan.create(scan.getCluster(), t, scan.getHints());
//      }
//    });

    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    UnaryOperator<SqlWriterConfig> transform = c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(1)
            .withSelectFolding(null);

    String sql = sqlNode.toSqlString(c -> transform.apply(c.withDialect(PostgresqlSqlDialect.DEFAULT)))
        .getSql();
    return sql;
  }
}
