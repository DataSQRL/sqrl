/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Deep-copies a SQL tree so that validation, which rewrites nodes in place, leaves the original
 * unparsing exactly as authored.
 */
public final class SqlNodeCopier extends SqlShuttle {

  public static SqlNode copy(SqlNode node) {
    return node.accept(new SqlNodeCopier());
  }

  @Override
  public SqlNode visit(SqlLiteral literal) {
    return literal.clone(literal.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    return id.clone(id.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    return type.clone(type.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlDynamicParam param) {
    return param.clone(param.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
    return intervalQualifier.clone(intervalQualifier.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlCall call) {
    var argHandler = new CallCopyingArgHandler(call, true);
    call.getOperator().acceptCall(this, call, false, argHandler);
    return argHandler.result();
  }

  @Override
  public SqlNode visit(SqlNodeList nodeList) {
    List<SqlNode> copies = new ArrayList<>(nodeList.size());
    for (SqlNode node : nodeList) {
      copies.add(node == null ? null : node.accept(this));
    }
    return new SqlNodeList(copies, nodeList.getParserPosition());
  }
}
