/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan;

import com.datasqrl.schema.TypeFactory;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;

public class SqlValidatorUtil {

  public static SqlValidator createSqlValidator(SqrlSchema schema,
      SqlOperatorTable operatorTable) {
    Properties p = new Properties();
    p.put(CalciteConnectionProperty.CASE_SENSITIVE.name(), false);

    SqlValidator validator = new FlinkCalciteSqlValidator(
        operatorTable,
        new SqrlCalciteCatalogReader(schema, List.of(), TypeFactory.getTypeFactory(),
            new CalciteConnectionConfigImpl(p).set(CalciteConnectionProperty.CASE_SENSITIVE,
                "false")),
        TypeFactory.getTypeFactory(),
        SqrlPlannerConfigFactory.sqlValidatorConfig
    );
    return validator;
  }
}
