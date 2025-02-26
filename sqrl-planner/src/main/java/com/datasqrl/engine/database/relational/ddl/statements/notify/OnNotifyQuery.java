package com.datasqrl.engine.database.relational.ddl.statements.notify;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

@AllArgsConstructor
public class OnNotifyQuery implements SqlDDLStatement {

  SqrlFramework framework;
  String tableName;
  List<Parameter> parameters;

  @Override
  public String getSql() {
    QueryPlanner planner = framework.getQueryPlanner();
    RelBuilder builder = planner.getRelBuilder();

    RelDataTypeFactory typeFactory = builder.getTypeFactory();
    RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    for (Parameter parameter : parameters) {
      fieldInfo.add(parameter.getName(), parameter.getRelDataTypeField().getType());
    }
    RelDataType tempSchema = fieldInfo.build();

    Consumer<RelBuilder> queryBuilder =
        relBuilder -> {
          // In the context of pg_notify, the payload is always a string. As part of generating a
          // parameterized query below, we appropriately cast this string back to its original type
          // when required.
          RelDataType parameterType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

          relBuilder.scan(tableName);
          for (int i = 0; i < parameters.size(); i++) {
            Parameter parameter = parameters.get(i);
            RexNode sqlParameter = relBuilder.getRexBuilder().makeDynamicParam(parameterType, i);
            RexNode castParameter =
                relBuilder
                    .getRexBuilder()
                    .makeCast(parameter.getRelDataTypeField().getType(), sqlParameter);
            relBuilder.filter(
                relBuilder.equals(relBuilder.field(parameter.getName()), castParameter));
          }
        };

    RelNode queryPlan = planner.planQueryOnTempTable(tempSchema, tableName, queryBuilder);

    SqlNodeToString.SqlStrings sqlStrings = QueryPlanner.relToString(Dialect.POSTGRES, queryPlan);
    return sqlStrings.getSql();
  }
}
