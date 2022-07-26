/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * Definition of the SQL <code>COUNT</code> aggregation function.
 *
 * <p><code>COUNT</code> is an aggregator which returns the number of rows which
 * have gone into it. With one argument (or more), it returns the number of rows
 * for which that argument (or all) is not <code>null</code>.
 */
public class SqlCountInlineAggFunction extends SqlFunction implements ConvertableFunction {

  private final SqlCountAggFunction fnc;
  //~ Constructors -----------------------------------------------------------

  public SqlCountInlineAggFunction(
      SqlCountAggFunction fnc, String name, SqlIdentifier sqlIdentifier, SqlKind kind,
      SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory category) {
    super(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference, operandTypeChecker,
        category);
    this.fnc = fnc;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION_STAR;
  }

  @SuppressWarnings("deprecation")
  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.ANY), true));
  }

  @SuppressWarnings("deprecation")
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.BIGINT);
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Check for COUNT(*) function.  If it is we don't
    // want to try and derive the "*"
    if (call.isCountStar()) {
      return validator.getTypeFactory().createSqlType(
          SqlTypeName.BIGINT);
    }
    return super.deriveType(validator, scope, call);
  }

  @Override
  public Optional<SqlAggFunction> revert() {
    return Optional.of(fnc);
  }

  @Override
  public Optional<SqlFunction> convertToInlineAgg() {
    return Optional.of(this);
  }
}
