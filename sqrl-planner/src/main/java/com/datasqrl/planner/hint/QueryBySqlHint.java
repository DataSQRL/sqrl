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
package com.datasqrl.planner.hint;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.graphql.server.query.SqlQueryModifier;
import com.datasqrl.graphql.server.query.SqlQueryModifier.UserSqlQuery;
import com.datasqrl.graphql.server.query.SqlQueryModifier.UserSqlQuery.Type;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.EnumUtil;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

/**
 * Defines a table access function that allows to query the underlying table/function by a sql
 * query.
 *
 * <p>TODO: - support TRANSFORM: The return type of the function becomes JSON which needs to be
 * supported through the GraphQL generation and execution phases - support re-using existing
 * parameters if they have the parameterName, this gives users full control over parameters. We
 * validate it's STRING and overwrite the description if absent. - add FULL: support sub-queries,
 * self-joins, and more that is not allowed for TRANSFORM
 */
@Getter
public class QueryBySqlHint extends ColumnNamesHint implements QueryByHint {

  public static final String HINT_NAME = "query_by_sql";
  public static final int MAX_SQL_LENGTH = 5000;

  public static final Map<UserSqlQuery.Type, String> PARAMETER_DESCRIPTION =
      Map.of(
          UserSqlQuery.Type.FILTER,
              "ANSI SQL where clause against the following table: %s. Only provide the WHERE condition not the whole query.",
          UserSqlQuery.Type.TRANSFORM,
              "ANSI SQL query against the following table without sub-queries or joins: %s");

  private final SqlQueryModifier.UserSqlQuery.Type queryType;
  private final String parameterName;

  protected QueryBySqlHint(
      ParsedObject<SqrlHint> source,
      SqlQueryModifier.UserSqlQuery.Type queryType,
      String parameterName) {
    super(source, Type.ANALYZER, List.of());
    this.queryType = queryType;
    this.parameterName = parameterName;
  }

  public SqrlTableFunction modifyFunction(
      SqrlTableFunction function, FlinkTypeFactory typeFactory) {
    Preconditions.checkArgument(
        queryType == UserSqlQuery.Type.FILTER,
        "Currently, only" + "filter SQL parameters are supported");
    String tableName = function.getSimpleName();
    String tableStmt =
        new FlinkTableBuilder()
            .setName(tableName)
            .setRelDataType(function.getRowType())
            .buildSql(false)
            .toString();
    UserSqlQuery modifier = new UserSqlQuery(queryType, tableName, parameterName, tableStmt);
    List<FunctionParameter> parameters = new ArrayList<>(function.getParameters());
    String parameterDescription = PARAMETER_DESCRIPTION.get(queryType).formatted(tableStmt);
    parameters.add(
        new SqrlFunctionParameter(
            parameterName,
            parameterDescription,
            parameters.size(),
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR, MAX_SQL_LENGTH), false),
            false,
            Optional.empty(),
            Optional.empty()));
    return new SqrlTableFunction(
        function.getFullPath(),
        parameters,
        function.getFunctionAnalysis(),
        function.getMultiplicity(),
        function.getVisibility(),
        List.of(modifier),
        function.getExecutableQuery(),
        function.getDocumentation());
  }

  @AutoService(Factory.class)
  public static class QueryBySqlFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var arguments = source.get().getOptions();
      if (arguments.size() != 2) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            HINT_NAME
                + " hint requires two arguments: the type of SQL query and the parameter name.");
      }
      var queryType =
          EnumUtil.getByName(SqlQueryModifier.UserSqlQuery.Type.class, arguments.get(0));
      if (queryType.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "Unknown sql query type: %s",
            arguments.get(0));
      }
      var parameterName = arguments.get(1).trim();
      if (parameterName.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC, source.getFileLocation(), "Parameter name cannot be empty");
      }
      return new QueryBySqlHint(source, queryType.get(), parameterName);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }
}
