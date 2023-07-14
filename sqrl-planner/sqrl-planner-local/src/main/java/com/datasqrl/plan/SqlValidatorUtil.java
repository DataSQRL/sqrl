/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan;

import com.datasqrl.schema.TypeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;

public class SqlValidatorUtil {


  public static CalciteCatalogReader createCatalogReader(SqrlSchema schema) {
    return new SqrlCalciteCatalogReader(
        schema,
        CalciteSchema.from(schema.plus()).path(null),
        SqrlPlannerConfigFactory.createSqrlTypeFactory(), null);
  }

  public static SqlValidator createSqlValidator(SqrlSchema schema,
      SqlOperatorTable operatorTable) {
    Properties p = new Properties();
    p.put(CalciteConnectionProperty.CASE_SENSITIVE.name(), false);


//    SqlOperatorTable opTab0 = SqlStdOperatorTable.instance();
    List<SqlOperatorTable> list = new ArrayList();
    list.add(operatorTable);
    list.add(createCatalogReader(schema));
    SqlOperatorTable opTab = SqlOperatorTables.chain(list);

    SqlValidator validator = new FlinkCalciteSqlValidator(
        opTab,
        new SqrlCalciteCatalogReader(schema, List.of(), TypeFactory.getTypeFactory(),
            new CalciteConnectionConfigImpl(p).set(CalciteConnectionProperty.CASE_SENSITIVE,
                "false")),
        TypeFactory.getTypeFactory(),
        SqrlPlannerConfigFactory.sqlValidatorConfig
    );
    return validator;
  }
}
