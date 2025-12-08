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
package com.datasqrl.function;

import com.datasqrl.util.FunctionUtil;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.Optionality;
import org.apache.flink.table.functions.FunctionDefinition;

@UtilityClass
public class CalciteFunctionUtil {

  /**
   * Operator that is used generally for rex planning where no operand inference or return type
   * inference happens.
   */
  public static SqlUnresolvedFunction lightweightOp(FunctionDefinition functionDefinition) {
    return lightweightOp(FunctionUtil.getFunctionName(functionDefinition));
  }

  public static SqlUnresolvedFunction lightweightOp(String name) {
    return new SqlUnresolvedFunction(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        null,
        null,
        null,
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  public static SqlUnresolvedFunction lightweightOp(
      String name, SqlReturnTypeInference returnType) {
    return new SqlUnresolvedFunction(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        returnType,
        null,
        null,
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION) {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return returnType.inferReturnType(opBinding);
      }
    };
  }

  public static SqlBinaryOperator lightweightBiOp(String name) {
    return new SqlBinaryOperator(
        name, SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.ANY), null, null);
  }

  public static SqlUserDefinedAggFunction lightweightAggOp(String name) {
    return new SqlUserDefinedAggFunction(
        new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        null,
        null,
        null,
        null,
        false,
        false,
        Optionality.IGNORED);
  }

  public static void writeFunction(String fnName, SqlWriter writer, SqlCall origCall) {
    writeFunction(fnName, writer, origCall.getOperandList());
  }

  public static void writeFunction(String fnName, SqlWriter writer, SqlNode... operands) {
    writeFunction(fnName, writer, List.of(operands));
  }

  public static void writeFunction(String fnName, SqlWriter writer, List<SqlNode> operands) {
    var fn = writer.startFunCall(fnName);
    writeOperands(writer, operands);
    writer.endFunCall(fn);
  }

  public static void writeOperands(SqlWriter writer, List<SqlNode> operands) {
    boolean first = true;
    for (SqlNode operand : operands) {
      if (!first) {
        writer.sep(",", true);
      }
      operand.unparse(writer, 0, 0);
      first = false;
    }
  }
}
