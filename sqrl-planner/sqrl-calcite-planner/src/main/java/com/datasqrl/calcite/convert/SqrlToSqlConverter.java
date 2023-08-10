package com.datasqrl.calcite.convert;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SqrlToSqlConverter {

  public static SqlNode convertToSql(SqlNode node, SchemaMetadata metadata) {
    Map<String, List<String>> aliasToPath = new HashMap<>();

    Stack<List<String>> lastPathSeen = new Stack<>();
    Stack<String> lastAliasSeen = new Stack<>();
    SqlNode rewritten = node.accept(new SqlShuttle() {

      @Override
      public SqlNode visit(SqlIdentifier id) {
        SqlNode node = convertToJoin(0, id.names, new ArrayList<>(), null, Optional.empty());
        SqlNode select = new SqlSelect(SqlParserPos.ZERO,
            null,
            new SqlNodeList(List.of(new SqlIdentifier(List.of(lastAliasSeen.pop(), ""),
                SqlParserPos.ZERO)), SqlParserPos.ZERO),
            node,
            null, null, null, SqlNodeList.EMPTY,
            null, null, null, SqlNodeList.EMPTY);
        return select;
      }

      @Override
      public SqlNode visit(SqlCall call) {
        switch (call.getKind()) {
          case OTHER_FUNCTION:
            if (!(call.getOperator() instanceof SqlUnresolvedFunction)) {
              break;
            }
            SqlUnresolvedFunction fnc = (SqlUnresolvedFunction) call.getOperator();
            List<String> path = fnc.getSqlIdentifier().names;
            SqlNode node1 = convertToJoin(0, path, new ArrayList<>(), null,
                Optional.of(call.getOperandList()));

            SqlNode select = new SqlSelect(SqlParserPos.ZERO,
                null,
                new SqlNodeList(List.of(new SqlIdentifier(List.of(lastAliasSeen.pop(), ""),
                    SqlParserPos.ZERO)), SqlParserPos.ZERO),
                node1,
                null, null, null, SqlNodeList.EMPTY,
                null, null, null, SqlNodeList.EMPTY);
            return select;
          case SELECT:
            return new SqlSelect(SqlParserPos.ZERO,
                call.operand(0),
                call.operand(1),
                call.operand(2).accept(this),
                call.operand(3),
                call.operand(4),
                call.operand(5),
                call.operand(6),
                call.operand(7),
                call.operand(8),
                call.operand(9),
                call.operand(10)
            );
          case JOIN:
            // If JOIN, walk left and right and rewrite the join with the result
            SqlNode leftNode = call.operand(0).accept(this);
            SqlNode rightNode = call.operand(3).accept(this);
            //todo lateral but check first
            return new SqlJoin(SqlParserPos.ZERO,
                leftNode,
                call.operand(1),
                call.operand(2),
                SqlStdOperatorTable.LATERAL.createCall(SqlParserPos.ZERO, rightNode), //all joins converted to later
                call.operand(4).equals(JoinConditionType.NONE.symbol(SqlParserPos.ZERO)) ? JoinConditionType.ON.symbol(SqlParserPos.ZERO) : call.operand(4)  ,
                call.operand(4).equals(JoinConditionType.NONE.symbol(SqlParserPos.ZERO)) ? SqlLiteral.createBoolean(true, SqlParserPos.ZERO) :call.operand(4));

          case AS:
            SqlNode rewritten = call.operand(0).accept(this);
            aliasToPath.put(((SqlIdentifier) call.getOperandList().get(1)).getSimple(),
                lastPathSeen.pop());

            return call.getOperator().createCall(call.getParserPosition(),
                rewritten, call.getOperandList().get(1) );
          case ORDER_BY:
            System.out.println();

            return call.getOperator().createCall(SqlParserPos.ZERO,
                call.operand(0).accept(this),
                call.operand(1),
                call.operand(2),
                call.operand(3));

          default:
            break;
        }
        return super.visit(call);
      }

      private SqlNode convertToJoin(int i, List<String> identifier, List<String> currentPath,
                                    String lastAlias, Optional<List<SqlNode>> operandList) {
        //has alias, append to absolute path and continue
        if (i == 0 && aliasToPath.get(identifier.get(i)) != null) {
          currentPath = aliasToPath.get(identifier.get(i));
          return convertToJoin(i+1, identifier, currentPath, identifier.get(i), operandList);
        }
        String alias = "alias" + i;

        if (identifier.size() - i < 2) {
          currentPath.add(identifier.get(i));
          SqlNode table = rewriteCorrelationFields(currentPath, lastAlias, operandList);
          walkPath(currentPath, identifier.get(i));
          lastPathSeen.push(currentPath);
          lastAliasSeen.push(alias);

          return SqlStdOperatorTable.AS.createCall(
              SqlParserPos.ZERO, table,
              new SqlIdentifier(alias, SqlParserPos.ZERO)); // If there's only one part, return as is
        }
        currentPath.add(identifier.get(i));
        SqlNode left = rewriteCorrelationFields(currentPath, lastAlias, Optional.empty());
        currentPath = walkPath(currentPath, identifier.get(i));

        left = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
            left, new SqlIdentifier(alias, SqlParserPos.ZERO));

        SqlNode right = convertToJoin(i + 1, identifier, currentPath, alias, operandList);
        return
            new SqlJoin(SqlParserPos.ZERO,
                left,
                SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                JoinType.INNER.symbol(SqlParserPos.ZERO),
                SqlStdOperatorTable.LATERAL.createCall(SqlParserPos.ZERO, right),
                JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                SqlLiteral.createBoolean(true, SqlParserPos.ZERO));
      }

      private SqlNode rewriteCorrelationFields(List<String> currentPath, String alias, Optional<List<SqlNode>> operandList) {
        SqlNode query = metadata.getNodeMapping().get(currentPath);
        AtomicInteger selfCount = new AtomicInteger(0);
        return query.accept(new SqlShuttle() {
          @Override
          public SqlNode visit(SqlCall call) {
            if (call.getKind() == SqlKind.AS) {
              return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                  call.getOperandList().get(0).accept(this), call.getOperandList().get(1));
            }
            return super.visit(call);
          }

          @Override
          public SqlNode visit(SqlIdentifier id) {
            if (id.names.get(0).equals("@")) {
              List<String> names = metadata.getTableArgPositions().get(currentPath);
              return new SqlIdentifier(List.of(alias, names.get(selfCount.getAndIncrement())), SqlParserPos.ZERO);
            }

            return super.visit(id);
          }

          @Override
          public SqlNode visit(SqlDynamicParam param) {
            SqlNode node = operandList.get().get(param.getIndex());
            return node;
          }
        });
      }

      private List<String> walkPath(List<String> currentPath, String next) {
        if (metadata.getJoinMapping().get(currentPath) != null) {
          currentPath = metadata.getJoinMapping().get(currentPath);
        }
        return currentPath;
      }

    });

    return rewritten;
  }
}
