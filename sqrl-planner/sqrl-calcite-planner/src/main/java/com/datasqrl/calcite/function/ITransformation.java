package com.datasqrl.calcite.function;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public interface ITransformation {
    SqlNode apply(String dialect, SqlOperator op, SqlParserPos pos, List<SqlNode> nodeList);
}
