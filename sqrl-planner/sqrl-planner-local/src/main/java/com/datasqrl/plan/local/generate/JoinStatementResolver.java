package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.calcite.SqlValidatorUtil;
import com.datasqrl.plan.local.transpile.ConvertJoinDeclaration;
import com.datasqrl.plan.local.transpile.JoinDeclarationUtil;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.JoinAssignment;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.validate.SqlValidator;

public class JoinStatementResolver extends AbstractStatementResolver {


  protected JoinStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner) {
    super(errors, nameCanonicalizer, planner);
  }

  public void resolve(JoinAssignment statement, Namespace ns) {
    SqlNode joinSqlNode = transpile(statement, ns);

    Optional<SQRLTable> tbl = getContext(ns, statement.getNamePath());

    final SqlNode fullSql = joinSqlNode.accept(
        new ConvertJoinDeclaration(tbl.map(SQRLTable::getVt)));
    RelNode relNode = plan(fullSql); //verify correct sql

    checkState(tbl.isPresent(), ErrorLabel.GENERIC, statement,
            "Root JOIN declarations are not yet supported");
    updateJoinMapping(ns, statement, joinSqlNode, fullSql);
  }

  private void updateJoinMapping(Namespace ns, JoinAssignment statement, SqlNode joinSqlNode,
      SqlNode fullSql) {
    //op is a join, we need to discover the /to/ relationship
    SQRLTable table = getContext(ns, statement.getNamePath())
        .orElseThrow(() -> new RuntimeException("Internal Error: Missing context"));
    JoinDeclarationUtil joinDeclarationUtil = new JoinDeclarationUtil(
        planner.createRelBuilder().getRexBuilder());

    SqlValidator validator = createValidator(ns);
    validator.validate(fullSql);
    SQRLTable toTable =
        joinDeclarationUtil.getToTable(validator,
            fullSql);
    Multiplicity multiplicity = getMultiplicity(fullSql);

    checkState(table.getField(statement.getNamePath().getLast()).isEmpty(),
        ErrorCode.CANNOT_SHADOW_RELATIONSHIP, statement);

    table.addRelationship(statement.getNamePath().getLast(), toTable,
        Relationship.JoinType.JOIN, multiplicity, Optional.of((SqrlJoinDeclarationSpec) joinSqlNode));
  }

  //todo: this is too many places
  private SqlValidator createValidator(Namespace ns) {
    return SqlValidatorUtil.createSqlValidator(ns.getSchema(),
        ns.getOperatorTable());
  }

  private Multiplicity getMultiplicity(SqlNode sql) {
    Optional<SqlNode> fetch = getFetch(sql);

    return fetch
        .filter(f -> ((SqlNumericLiteral) f).intValue(true) == 1)
        .map(f -> Multiplicity.ONE)
        .orElse(Multiplicity.MANY);
  }

  private Optional<SqlNode> getFetch(SqlNode sql) {
    if (sql instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) sql;
      return Optional.ofNullable(select.getFetch());
    } else if (sql instanceof SqlOrderBy) {
      SqlOrderBy order = (SqlOrderBy) sql;
      return Optional.ofNullable(order.fetch);
    } else {
      throw new RuntimeException();//fatal(env, "Unknown node type");
    }
  }
}
