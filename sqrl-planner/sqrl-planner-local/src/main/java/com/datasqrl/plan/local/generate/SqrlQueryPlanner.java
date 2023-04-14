package com.datasqrl.plan.local.generate;

import com.datasqrl.functions.SqrlFunctionCatalog;
import com.datasqrl.plan.OptimizationStage;
import com.datasqrl.plan.RelStageRunner;
import com.datasqrl.plan.SqlValidatorUtil;
import com.datasqrl.plan.SqrlRelBuilder;
import com.datasqrl.plan.SqrlToRelConverter;
import com.google.inject.Inject;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.RelNode;
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

    SqrlToRelConverter sqlToRelConverter = new SqrlToRelConverter(schema);

    return sqlToRelConverter.toRel(sqlValidator, validatedNode);
  }

  public RelBuilder createRelBuilder() {
    return SqrlRelBuilder.create(schema.getCluster(), schema);
  }

  public RelNode plan(SqlNode sqlNode) {
    SqlValidator validator = createValidator();
    validator.validate(sqlNode);

    SqrlToRelConverter relConverter = new SqrlToRelConverter(schema);
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
