package com.datasqrl.engine.database.relational.ddl.statements.notify;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
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
    RelDataType parameterType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    builder.scan(tableName);
    for (int i = 0; i < parameters.size(); i++) {
      Parameter parameter = parameters.get(i);
      RexNode sqlParameter = builder.getRexBuilder().makeDynamicParam(parameterType, i);
      RexNode castParameter = builder.getRexBuilder().makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER), sqlParameter);
      builder.filter(builder.equals(builder.field(parameter.getName()), castParameter));
    }

    RelNode filteredNode = builder.build();

    RelToSqlNode.SqlNodes sqlNodes = planner.relToSql(Dialect.POSTGRES, filteredNode);

    SqlNodeToString.SqlStrings sqlStrings = planner.sqlToString(Dialect.POSTGRES, sqlNodes);

    return sqlStrings.getSql();
  }

}
