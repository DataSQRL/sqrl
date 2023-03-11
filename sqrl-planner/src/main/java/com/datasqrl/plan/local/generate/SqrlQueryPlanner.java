package com.datasqrl.plan.local.generate;

import static java.util.Objects.requireNonNull;

import com.datasqrl.functions.SqrlFunctionCatalog;
import com.datasqrl.plan.calcite.OptimizationStage;
import com.datasqrl.plan.calcite.RelStageRunner;
import com.datasqrl.plan.calcite.SqlValidatorUtil;
import com.datasqrl.plan.calcite.SqrlRelBuilder;
import com.datasqrl.plan.calcite.SqrlToRelConverter;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.calcite.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.calcite.rules.SqrlRelMetadataQuery;
import com.google.inject.Inject;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;

/**
 * A facade to the calcite planner
 */
@Getter
public class SqrlQueryPlanner {
  private final SqrlSchema schema;
  private final SqrlFunctionCatalog functionCatalog;

  @Inject
  public SqrlQueryPlanner(SqrlSchema schema, SqrlFunctionCatalog functionCatalog) {
    this.schema = schema;
    this.functionCatalog = functionCatalog;
  }

  public RelNode planQuery(SqlNode node) {
    SqlValidator sqlValidator = SqlValidatorUtil.createSqlValidator(schema,
        functionCatalog.getOperatorTable());
    SqlNode validatedNode = sqlValidator.validate(node);

    SqrlToRelConverter sqlToRelConverter = new SqrlToRelConverter(schema.getCluster(),
        schema);

    return sqlToRelConverter.toRel(sqlValidator, validatedNode);
  }

  public RelBuilder createRelBuilder() {
    return SqrlRelBuilder.create(schema.getCluster(), schema);
  }

  public RelNode plan(SqlNode sqlNode) {
    SqlValidator validator = createValidator();
    validator.validate(sqlNode);

    SqrlToRelConverter relConverter = new SqrlToRelConverter(schema.getCluster(),
        schema);
    RelNode relNode = relConverter.toRel(validator, sqlNode);
    return relNode;
  }

  private SqlValidator createValidator() {
    return SqlValidatorUtil.createSqlValidator(schema,
        functionCatalog.getOperatorTable());
  }

  public SqrlSchema getSchema() {
    return schema;
  }

  public RelNode runStage(OptimizationStage stage, RelNode relNode) {
    return RelStageRunner.runStage(stage,
        relNode, schema.getPlanner());
  }
}
