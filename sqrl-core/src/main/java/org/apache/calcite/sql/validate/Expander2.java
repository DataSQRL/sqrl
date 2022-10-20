package org.apache.calcite.sql.validate;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

/**
 * Converts an expression into canonical form by fully-qualifying any identifiers.
 */
public class Expander2 extends SqlScopedShuttle {

  protected final SqlValidatorImpl validator;

  public Expander2(SqlValidatorImpl validator, SqlValidatorScope scope) {
    super(scope);
    this.validator = validator;
  }

  @Override public SqlNode visit(SqlIdentifier id) {
    // First check for builtin functions which don't have
    // parentheses, like "LOCALTIME".
    final SqlCall call = validator.makeNullaryCall(id);
    if (call != null) {
      return call.accept(this);
    }
    final SqlIdentifier fqId = getScope().fullyQualify(id).identifier;
    SqlNode expandedExpr = expandDynamicStar(id, fqId);
    validator.setOriginal(expandedExpr, id);
    return expandedExpr;
  }
//
//  @Override
//  public SqlNode visit(SqlIdentifier id) {
//    // First check for builtin functions which don't have
//    // parentheses, like "LOCALTIME".
//    final SqlCall call = validator.makeNullaryCall(id);
//    if (call != null) {
//      return call.accept(this);
//    }
//
//    final SqlIdentifier fqId = get(id);
////
////
////    //check to see if we're inside a path so we can reorient it to a different  scope
////    if (fqId.names.size() > 2) {
////      SqlPathIdentifier table = new SqlPathIdentifier(
////          fqId.names.subList(0, fqId.names.size() - 1),
////          null,
////          fqId.getParserPosition(),
////          IntStream.range(0, fqId.names.size() - 1)
////              .mapToObj(i -> fqId.getComponentParserPosition(i))
////              .collect(Collectors.toList()));
////      System.out.println(table);
////
//////      SqlValidatorScope scope = ((SqrlValidatorImpl)validator).getScopeForAlias(fqId.names.get(0));
////      SqlPathJoin j = new SqlPathJoin(fqId.names.subList(0, fqId.names.size() - 1),
////         SqlParserPos.ZERO, IntStream.range(0, fqId.names.size() - 1)
////          .mapToObj(i -> fqId.getComponentParserPosition(i))
////          .collect(Collectors.toList()));
////
////      SqlValidatorNamespace ns= ((SqrlValidatorImpl)validator).getJoinScopes().get(fqId.names.get(0));
////
////      SqlIdentifier sqlIdentifier = new SqlIdentifierWithPath(table,
////          List.of(
////              fqId.names.subList(0, fqId.names.size() - 1).stream()
////                      .collect(Collectors.joining(".")),
////              fqId.names.get(fqId.names.size()-1)
////          ), null,
////          SqlParserPos.ZERO,
////          IntStream.range(0, fqId.names.size() - 1)
////              .mapToObj(i -> fqId.getComponentParserPosition(i))
////              .collect(Collectors.toList())
////      );
////      validator.registerFrom(
////          getScope(),
////          null,
////          false,
////          j,
////          null,
////          null,
////          null,
////          false,
////          false);
////      validator.namespaces.put(table, ns);
////      table.setSqlPathJoin(j);
////      final JoinScope joinScope =
////          new JoinScope(getScope(), null, j);
////      validator.scopes.put(j, joinScope);
////      validator.validateJoin(table.getSqlPathJoin(), getScope());
////      validator.getNamespace(table.getSqlPathJoin().getLeft()).validate(
////          ((SqrlValidatorImpl) validator).unknownType);
////      validator.getNamespace(table.getSqlPathJoin().getRight()).validate(
////          ((SqrlValidatorImpl) validator).unknownType);
////      return sqlIdentifier;
////
//////      validator.validateFrom(table, validator.unknownType, getScope());
////
//////      validateFrom()
////    }
//
//    SqlNode expandedExpr = expandDynamicStar(id, fqId);
//    validator.setOriginal(expandedExpr, id);
//    return expandedExpr;
//  }

  private SqlIdentifier get(SqlIdentifier id) {
    return getScope().fullyQualify(id).identifier;
  }

  @Override
  protected SqlNode visitScoped(SqlCall call) {
    switch (call.getKind()) {
      case SCALAR_QUERY:
      case CURRENT_VALUE:
      case NEXT_VALUE:
      case WITH:
        return call;
    }
    // Only visits arguments which are expressions. We don't want to
    // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
    ArgHandler<SqlNode> argHandler =
        new CallCopyingArgHandler(call, false);
    call.getOperator().acceptCall(this, call, true, argHandler);
    final SqlNode result = argHandler.result();
    validator.setOriginal(result, call);
    return result;
  }

  protected SqlNode expandDynamicStar(SqlIdentifier id, SqlIdentifier fqId) {
    if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
        && !DynamicRecordType.isDynamicStarColName(Util.last(id.names))) {
      // Convert a column ref into ITEM(*, 'col_name')
      // for a dynamic star field in dynTable's rowType.
      SqlNode[] inputs = new SqlNode[2];
      inputs[0] = fqId;
      inputs[1] = SqlLiteral.createCharString(
          Util.last(id.names),
          id.getParserPosition());
      return new SqlBasicCall(
          SqlStdOperatorTable.ITEM,
          inputs,
          id.getParserPosition());
    }
    return fqId;
  }
}