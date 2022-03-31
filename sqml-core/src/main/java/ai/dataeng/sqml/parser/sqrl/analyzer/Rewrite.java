package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.Limit;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import java.util.Optional;

public class Rewrite {
//
//  private Relation rewriteFrom(Relation from) {
//    TableUnsqrlVisitor tableUnsqrlVisitor = new TableUnsqrlVisitor(this);
//    return from.accept(tableUnsqrlVisitor, null)
//        .current.get();
//  }

  private Select rewriteSelect(Select select) {
    return select;
  }
  private Optional<Expression> rewriteWhere(Optional<Expression> where) {
    return where.map( w -> ExpressionTreeRewriter.rewriteWith(new AliasRewriter(), w));
  }
  private Optional<GroupBy> rewriteGroupBy(Optional<GroupBy> groupBy) {
//    if (((SqrlValidator)validator).hasAgg(query.getSelectList())) {
//      List<SqlNode> nodes = new ArrayList<>();
//      if (group != null) {
//        nodes.addAll(group.getList());
//      }
//      List<Column> primaryKeys = this.contextTable.get().getPrimaryKeys();
//      for (int i = primaryKeys.size() - 1; i >= 0; i--) {
//        Column column = primaryKeys.get(i);
//        nodes.add(0, ident("_", column.getId().toString()));
//      }
//      return (SqlNodeList)new SqlNodeList(nodes, SqlParserPos.ZERO).accept(new ai.dataeng.sqml.parser.macros.AliasRewriter(mapper));
//    }
//
//    if (group != null) {
//      return (SqlNodeList)group.accept(new ai.dataeng.sqml.parser.macros.AliasRewriter(mapper));
//    }
//    return group;
    return groupBy;
  }
  private Optional<Expression> rewriteHaving(Optional<Expression> having) {
    return having.map( h -> ExpressionTreeRewriter.rewriteWith(new AliasRewriter(), h));
  }
  private Optional<OrderBy> rewriteOrderBy(Optional<OrderBy> orderBy) {
//    if (orderList != null && ((SqrlValidator)validator).hasAgg(query.getSelectList())) {
//      List<SqlNode> nodes = new ArrayList<>(orderList.getList());
//      //get parent primary key for context
//      List<Column> primaryKeys = contextTable.get().getPrimaryKeys();
//      for (int i = primaryKeys.size() - 1; i >= 0; i--) {
//        Column pk = primaryKeys.get(i);
//        nodes.add(0, ident("_", pk.getId().toString()));
//      }
//      return new SqlNodeList(nodes, SqlParserPos.ZERO);
//    }
//
//    return orderList;
    return orderBy;
  }
  private Optional<Limit> rewriteLimit(Optional<Limit> limit) {
    return limit;
  }
  public class AliasRewriter extends ExpressionRewriter {

  }
}
