package com.datasqrl.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class PositionAdjustingSqlShuttle extends SqlShuttle {

  private final int columnOffset;
  private final int rowOffset;

  public PositionAdjustingSqlShuttle(int rowOffset, int columnOffset) {
    this.columnOffset = columnOffset;
    this.rowOffset = rowOffset;
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
    return adjustPosition(pos, rowOffset, columnOffset);
  }

  public static SqlParserPos adjustPosition(SqlParserPos pos, int rowOffset, int columnOffset) {
    int adjustedLine = pos.getLineNum() + rowOffset - 1;  // subtract 1 to start from 0
    int adjustedColumn = pos.getColumnNum() + (pos.getLineNum() == 1 ? columnOffset : 0) - 1;  // subtract 1 to start from 0
    int adjustedEndLine = pos.getEndLineNum() + rowOffset - 1;
    int adjustedEndColumn = pos.getEndColumnNum() + (pos.getLineNum() == 1 ? columnOffset : 0) - 1;
    return new SqlParserPos(adjustedLine, adjustedColumn, adjustedEndLine, adjustedEndColumn);
  }
}
