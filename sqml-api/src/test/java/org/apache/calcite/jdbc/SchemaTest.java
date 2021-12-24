package org.apache.calcite.jdbc;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.SneakyThrows;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.PlannerImpl2;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.OrdersSchema;
import org.apache.calcite.rules.SqrlTableExpander;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
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
        .withConformance(SqlConformanceEnum.LENIENT)
        .withIdentifierMaxLength(256);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(nonCachingSchema.plus())
        .parserConfig(parserConfig)
        .sqlToRelConverterConfig(SqlToRelConverter.config()
//            .withRelBuilderFactory(new RelBuilderFactory() {
//              @Override
//              public RelBuilder create(RelOptCluster relOptCluster, RelOptSchema relOptSchema) {
//
//                return new SqrlRelBuilder(null, relOptCluster, relOptSchema, null);
//              }
//            })
            .withRelBuilderConfigTransform(e-> {
              return RelBuilder.Config.DEFAULT.withBloat(-100);
            }))
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(Programs.ofRules())
        .build();

    this.planner = new PlannerImpl2(config);
//    Planner planner = Frameworks.getPlanner(config);
    this.planner = planner;
  }

  @Test
  @SneakyThrows
  public void testSchemaBuilding() {
    String query = ""
        + "SELECT e.`orders.parent.test.b` AS x, f.`orders.parent.test.c` AS y "
        + "FROM `Orders.entries` AS e INNER JOIN `@.entries` AS f ON true";
//
//    String query = ""
//          + "SELECT productid, `product.category` AS category,\n"
//          + "  sum(quantity) AS quantity, count(*) AS num_orders\n"
//          + "FROM `@.orders.entries`\n"
//          + "WHERE `parent.time` > TIMESTAMP '2004-10-19 10:23:54'\n"
//          + "GROUP BY productid, `category`\n"
//          + "ORDER BY num_orders DESC, quantity DESC";

    SqlNode parse = planner.parse(query);

    System.out.println();
    SqlNode validate = planner.validate(parse);
//Todo: after validation, run a new parser w/ fields known apriori
    RelRoot planRoot = planner.rel(validate);

    System.out.println(validate);
    System.out.println(planRoot.project().getRowType());
    System.out.println();
    System.out.println(RuleTest.convertToSql(planRoot.project()));
    System.out.println(planRoot.project().explain());

    HepProgram hep = HepProgram.builder()
        .addRuleClass(SqrlTableExpander.class)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hep);
    hepPlanner.addRule(SqrlTableExpander.Config.DEFAULT.toRule());
    hepPlanner.setRoot(planRoot.rel);

    RelNode rewritten = hepPlanner.findBestExp();
    System.out.println(RuleTest.convertToSql(rewritten));


  }
}