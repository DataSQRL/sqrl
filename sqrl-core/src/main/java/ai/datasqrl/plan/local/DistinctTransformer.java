package ai.datasqrl.plan.local;

import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyzer.QueryGenerator.Scope;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

/**
 * Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;
 *
 * SQL translation:
 * SELECT [column_list]
 * FROM (
 *    SELECT [column_list],
 *      ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
 *        ORDER BY time_attr [asc|desc]) AS rownum
 *    FROM table_name)
 * WHERE rownum = 1
 *
 */
public class DistinctTransformer extends QueryTransformer {

  public static SqlNode transform(SqlSelect select, List<Name> primaryKeys,
      Optional<Object> optional) {
    return null;
  }

  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignment node, Scope scope) {
    ResolvedNamePath namePath = scope.getAnalysis().getResolvedNamePath().get(node.getTableNode());
    AbstractSqrlTable table = scope.getCalcite().getTables().get(namePath.getToTable().getId());

    //
    //Convert query to sqlnode immediately, apply transforms w/ loaded statements
    //
    // Write out definitive list of all transforms, figure out approach
    // Some things logical plans are easier, other SQL nodes are easier
    // Which is which?
    //
    //
    // 1. Distinct ON -> Row num limit 1
    // 2. LIMIT 1 on Nested function -> row num
    // 3. Convert agg to propagate timestamp & other info -> rank
    // 4. inline aggregate -> break into subquery, reference column as id
    // 5. Add SELF join if we don't already have one
    // 6. Expand paths to joins on primary keys




    //    RelDataType type = table.getRowType(null);
//    List<RelDataTypeField> fields = type.getFieldList();
//
//    SqlNode distinctNode = SqlNodeUtil.distinctOn();
//
//    new SqlNodeList()
//        .collect(Collectors.toList()), pos.getPos(node.getLocation()));
//
//    ResolvedNamePath resolvedTable = analysis.getResolvedNamePath().get(node);
//    String name = resolvedTable.getToTable().getId().getCanonical();
//
////    if (node.getAlias().isPresent()) {
////      SqlIdentifier table = new SqlIdentifier(List.of(name), pos.getPos(node.getLocation()));
////      SqlNode[] operands = {table,
////          new SqlIdentifier(node.getAlias().get().getCanonical(), SqlParserPos.ZERO)};
////      return new SqlBasicCall(AS, operands, pos.getPos(node.getLocation()));
////    }
//
//    return new SqlIdentifier(List.of(name), pos.getPos(node.getLocation()));

    //1. Transform namePath into joins
    //2.

    return null;
  }
}
