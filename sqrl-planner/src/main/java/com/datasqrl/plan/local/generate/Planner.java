package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.calcite.SqlValidatorUtil;
import com.datasqrl.plan.calcite.SqrlToRelConverter;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

public class Planner {

  public Planner(SystemContext systemContext) {

  }

  public RelNode plan(SqlNode sqlNode, FlinkNamespace ns) {
    SqlValidator validator = createValidator(ns);
    validator.validate(sqlNode);

    SqrlToRelConverter relConverter = new SqrlToRelConverter(ns.session.getCluster(),
        ns.session.getSchema());
    RelNode relNode = relConverter.toRel(validator, sqlNode);
    return relNode;
  }

  private SqlValidator createValidator(FlinkNamespace ns) {
    return SqlValidatorUtil.createSqlValidator(ns.getSchema(),
        ns.getOperatorTable());
  }
}
