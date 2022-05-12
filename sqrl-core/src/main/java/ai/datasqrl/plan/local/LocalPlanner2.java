package ai.datasqrl.plan.local;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.physical.util.RelToSql;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeFormatter;
import ai.datasqrl.schema.Schema;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SqrlCalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;


@Slf4j
public class LocalPlanner2 extends AstVisitor<Void, Void> {

  @Getter
  private final CalcitePlanner calcitePlanner;

  public LocalPlanner2(Schema schema) {
    calcitePlanner = new CalcitePlanner(new SqrlCalciteSchema(schema));
  }

  public RelNode plan(SqlNode sqlNode) {
    SqlValidator validator = this.calcitePlanner.createValidator();
    SqlNode validated = validator.validate(sqlNode);
    RelNode relNode = this.calcitePlanner.plan(validated, validator);
    System.out.println("Validated: " + RelToSql.convertToSql(relNode));
    return relNode;
  }
}
