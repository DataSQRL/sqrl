package com.datasqrl.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class PositionAdjustingSqlShuttle extends SqlShuttle {

  private final SqlParserPos offset;
  private final SqlParserPos start;

  public PositionAdjustingSqlShuttle(SqlParserPos offset,
      SqlParserPos start) {
    this.offset = offset;
    this.start = start;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    List<SqlNode> adjustedOperands = call.getOperandList().stream()
        .map(operand -> operand == null ? null : operand.accept(this))
        .collect(Collectors.toList());
    SqlParserPos adjustedPos = adjustPosition(call.getParserPosition());

    //Hints are special, otherwise they get rewritten as basic calls
    if (call instanceof SqlHint) {
      SqlHint hint = (SqlHint) call;
      return new SqlHint(adjustedPos,
          (SqlIdentifier) hint.getOperandList().get(0).accept(this),
          (SqlNodeList)hint.getOperandList().get(1).accept(this),
          hint.getOptionFormat());
    }

    return call.getOperator()
        .createCall(adjustedPos, adjustedOperands);
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    return type.clone(adjustPosition(type.getParserPosition()));
  }

  @Override
  public SqlNode visit(SqlDynamicParam param) {
    return param.clone(adjustPosition(param.getParserPosition()));
  }

  @Override
  public SqlNode visit(SqlIntervalQualifier node) {
    return node.clone(adjustPosition(node.getParserPosition()));
  }

  @Override
  public SqlNode visit(SqlNodeList nodeList) {
    List<SqlNode> newNodes = nodeList.getList().stream()
        .map(node -> node == null ? null : node.accept(this))
        .collect(Collectors.toList());
    return new SqlNodeList(newNodes, adjustPosition(nodeList.getParserPosition()));
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    List<SqlParserPos> components = new ArrayList<>();
    for (int i = 0; i < id.names.size(); i++) {
      components.add(adjustPosition(id.getComponentParserPosition(i)));
    }

    return new SqlIdentifier(id.names, id.getCollation(), adjustPosition(id.getParserPosition()),
        components);
  }

  @Override
  public SqlNode visit(SqlLiteral literal) {
    return literal.clone(adjustPosition(literal.getParserPosition()));
  }

  public SqlParserPos adjustPosition(SqlParserPos pos) {
    return adjustPosition(offset, start, pos);
  }

  public static SqlParserPos adjustSinglePosition(SqlParserPos offset, SqlParserPos pos) {
    //There is no visibility into the start location
    int newLine;
    int newCol;
    if (pos.getLineNum() == 1) {
      newLine = offset.getLineNum();
      newCol = offset.getColumnNum() + pos.getColumnNum() - 1;
    } else {
      newLine = offset.getLineNum() + pos.getLineNum();
      newCol = pos.getColumnNum();
    }
    return new SqlParserPos(newLine, newCol);
  }

  public static SqlParserPos adjustPosition(SqlParserPos offset, SqlParserPos start, SqlParserPos pos) {
    int newLine;
    int newCol;
    if (start.getLineNum() == pos.getLineNum()) {
      newLine = offset.getLineNum();
      newCol = offset.getColumnNum() + (pos.getColumnNum() - start.getColumnNum());
    } else {
      newLine = offset.getLineNum() +  (pos.getLineNum() - start.getLineNum());
      newCol = pos.getColumnNum();
    }
    return new SqlParserPos(newLine, newCol);
  }

}
