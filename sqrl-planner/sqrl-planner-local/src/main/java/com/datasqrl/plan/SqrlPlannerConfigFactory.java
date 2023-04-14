/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan;

import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.schema.TypeFactory;
import lombok.AllArgsConstructor;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

@AllArgsConstructor
public class SqrlPlannerConfigFactory {
  public static SqlValidator.Config sqlValidatorConfig = SqlValidator.Config.DEFAULT
      .withCallRewrite(true)
      .withIdentifierExpansion(false)
      .withColumnReferenceExpansion(true)
      .withTypeCoercionEnabled(true) //must be true to allow null literals
      .withLenientOperatorLookup(false)
      .withSqlConformance(SqrlConformance.INSTANCE);

  public static final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
      .config()
      .withExpand(false)
      .withDecorrelationEnabled(false)
      .withTrimUnusedFields(false)
      .withHintStrategyTable(SqrlHintStrategyTable.getHintStrategyTable());


  public static CalciteCatalogReader createCatalogReader(SqrlSchema schema) {
    return new SqrlCalciteCatalogReader(
        schema,
        CalciteSchema.from(schema.plus()).path(null),
        SqrlPlannerConfigFactory.createSqrlTypeFactory(), null);
  }

  public static SqlRexConvertletTable createConvertletTable() {
    return StandardConvertletTable.INSTANCE;
  }

  public static RelDataTypeFactory createSqrlTypeFactory() {
    return TypeFactory.getTypeFactory();
  }
}
