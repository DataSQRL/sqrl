package com.datasqrl.calcite;

import com.datasqrl.calcite.function.vector.TextSearch;
import com.datasqrl.flink.FlinkConverter;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;


class RelToSqlTransformTest {

  private SqrlFramework framework;
  private RelBuilder relBuilder;
  private FlinkConverter flinkConverter;
  private RexBuilder rexBuilder;
  private RelToSqlConverter converter;

  @BeforeEach
  public void setup() {
    this.framework = CalciteTestUtil.createEcommerceFramework();
    this.relBuilder = framework.getQueryPlanner().createRelBuilder();
    this.rexBuilder = framework.getQueryPlanner().createRexBuilder();
    this.flinkConverter = new FlinkConverter(
        framework.getQueryPlanner().createRexBuilder(),
        framework.getQueryPlanner().getTypeFactory());
    converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);

  }

  @Test
  public void whereTest() {
    SqlFunction fnc = flinkConverter.convertFunction("textsearch", "textsearch",
        new TextSearch());
    this.framework.getSqrlOperatorTable().addFunction("textsearch", fnc);

    RelNode relNode = relBuilder.scan("ENTRIES$")
        .filter(
            relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
                relBuilder.call(fnc,
                 rexBuilder.makeLiteral("query"), rexBuilder.makeLiteral("param1")),
                    rexBuilder.makeZeroLiteral(framework.getTypeFactory().createSqlType(SqlTypeName.BIGINT))))
        .build();

    List<RelRule> rules = new TextSearch().transform(Dialect.POSTGRES, fnc);

    relNode = Programs.hep(rules, false, null)
        .run(framework.getQueryPlanner().getPlanner(), relNode, relNode.getTraitSet(),
            List.of(), List.of());

    System.out.println(converter.visitRoot(relNode).asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT));
  }

  @Test
  public void orderTest() {
    SqlFunction fnc = flinkConverter.convertFunction("textsearch", "textsearch",
        new TextSearch());
    this.framework.getSqrlOperatorTable().addFunction("textsearch", fnc);

    RelNode relNode = relBuilder.scan("ENTRIES$")
        .project(
                relBuilder.call(fnc,
                 rexBuilder.makeLiteral("query"), rexBuilder.makeLiteral("param1")))
        .sort(0)
        .build();

    List<RelRule> rules = new TextSearch().transform(Dialect.POSTGRES, fnc);

    relNode = Programs.hep(rules, false, null)
        .run(framework.getQueryPlanner().getPlanner(), relNode, relNode.getTraitSet(),
            List.of(), List.of());

    System.out.println(converter.visitRoot(relNode).asStatement().toSqlString(PostgresqlSqlDialect.DEFAULT));
  }
}