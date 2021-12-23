package org.apache.calcite.jdbc;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.SneakyThrows;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.OrdersSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SchemaTest {

  private Planner planner;

  @BeforeEach
  public void setup() {
    LogicalPlan logicalPlan = new LogicalPlan();
    SqrlToCalciteTableTranslator tableTranslator = new SqrlToCalciteTableTranslator();
    TableResolver tableResolver = new SchemaWalker(NamePath.parse("orders"), logicalPlan, tableTranslator);

    ContextAwareCalciteSchema nonCachingSchema = new ContextAwareCalciteSchema(null, new OrdersSchema(tableResolver), "");

    Config parserConfig = SqlParser.config()
        .withParserFactory(SqlParser.config().parserFactory())
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(nonCachingSchema.plus())
        .parserConfig(parserConfig)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(Programs.ofRules())
        .build();

    Planner planner = Frameworks.getPlanner(config);
    this.planner = planner;
  }

  @Test
  @SneakyThrows
  public void testSchemaBuilding() {
    String query = ""
        + "SELECT e.orders.parent.test.b as X, f.orders.parent.test.b as Y "
        + "FROM `@.entries` AS e INNER JOIN `@.entries` AS f ON TRUE "
        + "WHERE e.orders.parent.test.b = false";


    SqlNode parse = planner.parse(query);

    System.out.println();
    SqlNode validate = planner.validate(parse);

    RelRoot planRoot = planner.rel(validate);

    System.out.println(validate);
    System.out.println(planRoot.project().getRowType());
    System.out.println();
  }
}