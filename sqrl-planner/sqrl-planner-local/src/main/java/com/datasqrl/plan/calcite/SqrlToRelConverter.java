package com.datasqrl.plan.calcite;

import lombok.AllArgsConstructor;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;

@AllArgsConstructor
public class SqrlToRelConverter {
  SqrlSchema schema;

  public RelNode toRel(SqlValidator validator, SqlNode sqlNode) {
    final SqlToRelConverter.Config config = SqrlPlannerConfigFactory.
        sqlToRelConverterConfig.withTrimUnusedFields(false);
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter((ViewExpander)(a, b, c, d)->{return null;}, validator,
            SqrlPlannerConfigFactory.createCatalogReader(schema), schema.getCluster(),
            SqrlPlannerConfigFactory.createConvertletTable(), config);
    RelRoot root;
    root = sqlToRelConverter.convertQuery(sqlNode, false, true);
//    root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
    final RelBuilder relBuilder =
        config.getRelBuilderFactory().create(schema.getCluster(), null);
    root = root.withRel(
        RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    return root.rel;
  }
}
