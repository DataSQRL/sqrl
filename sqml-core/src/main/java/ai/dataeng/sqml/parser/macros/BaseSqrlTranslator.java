//package ai.dataeng.sqml.parser.macros;
//
//import ai.dataeng.sqml.parser.macros.SqrlTranslator.Scope;
//import org.apache.calcite.sql.SqlBasicCall;
//import org.apache.calcite.sql.SqlIdentifier;
//import org.apache.calcite.sql.SqlJoin;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.SqlOrderBy;
//import org.apache.calcite.sql.SqlSelect;
//
//public abstract class BaseSqrlTranslator {
//  public Scope visit(SqlNode node, Scope scope) {
//    if (node instanceof SqlOrderBy) {
//      return visitQuery((SqlSelect)((SqlOrderBy)node).query, null);
//    } else if (node instanceof SqlSelect) {
//      return visitQuery((SqlSelect) node, scope);
//    }
//    throw new RuntimeException("Unknown SqlNode type:" + node.getClass().getName());
//  }
//
//  public Scope visitFrom(SqlNode from, Scope scope) {
//    if (from instanceof SqlJoin) {
//      return visitJoin((SqlJoin) from, scope);
//    } else if (from instanceof SqlBasicCall
//        && ((SqlBasicCall) from).getOperandList().get(0) instanceof SqlIdentifier) {
//      return visitTable((SqlBasicCall) from, scope);
//    } else if (from instanceof SqlBasicCall
//        && ((SqlBasicCall) from).getOperandList().get(0) instanceof SqlSelect) {
//      return visitSubquery((SqlBasicCall) from, scope);
//    }
//    throw new RuntimeException("Unknown from clause: " + from);
//  }
//
//  protected abstract Scope visitQuery(SqlSelect query, Scope scope);
//  protected abstract Scope visitJoin(SqlJoin join, Scope scope);
//  protected abstract Scope visitTable(SqlBasicCall table, Scope scope);
//  protected abstract Scope visitSubquery(SqlBasicCall table, Scope scope);
//}
