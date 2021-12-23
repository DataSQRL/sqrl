package org.apache.calcite.jdbc;

import java.util.function.UnaryOperator;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.junit.jupiter.api.Test;

public class RuleTest {

  @Test
  @SneakyThrows
  public void testRuleRewriting() {
    String query = ""
        + "SELECT uuid, sum(quantity) total_quantity "
        + "FROM `default_schema`.`orders_entries` "
        + "GROUP BY uuid "
        + "ORDER BY uuid DESC";

    RelRule rule = new RemoveSortRule(RemoveSortRule.Config.DEFAULT);

    RelNode optimizedNode = rewrite(query, rule);
    String sql = convertToSql(optimizedNode);

    System.out.println(sql);
  }

  @Test
  @SneakyThrows
  public RelNode rewrite(String query, RelRule... rules) {

    SchemaPlus schema = Frameworks.createRootSchema(false);
    schema.add("default_schema", new ReflectiveSchema(new C360()));

    SqlConformance conformance = FlinkSqlConformance.DEFAULT;

    Config parserConfig = SqlParser.config()
        .withParserFactory(FlinkSqlParserImpl.FACTORY)
        .withConformance(conformance)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(schema)
        .parserConfig(parserConfig)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(Programs.ofRules(rules))
        .build();

    Planner planner = Frameworks.getPlanner(config);

    SqlNode parse = planner.parse(query);

    SqlNode validate = planner.validate(parse);
    RelRoot planRoot = planner.rel(validate);

    HepProgram hep = HepProgram.builder()
        .addRuleClass(RemoveSortRule.class)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hep);
    hepPlanner.addRule(new RemoveSortRule(RemoveSortRule.Config.DEFAULT));
    hepPlanner.setRoot(planRoot.rel);

    return hepPlanner.findBestExp();
  }

  public static String convertToSql(RelNode optimizedNode) {

    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    UnaryOperator<SqlWriterConfig> transform = c ->
        c.withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0);

    String sql = sqlNode.toSqlString(c -> transform.apply(c.withDialect(PostgresqlSqlDialect.DEFAULT)))
        .getSql();
    return sql;
  }

  public static class C360 {
    public final Order[] orders = {
        new Order("65f885d6-cba5-43c2-a272-a2c0a661f9a2", 10007543, 1000101, 1617774237, "2021-10-29 22:05:14.365038-07"),
        new Order("88cd1b18-8488-4e67-aaab-1f0decbbe832", 10008434, 1000107, 1617619797, "2021-10-29 22:05:14.365038-07"),
        new Order("28c1278d-5df9-4f28-b033-3a850f53bafa", 10008231, 1000101, 1617334664, "2021-10-29 22:05:14.365038-07"),
        new Order("3037f704-715f-4330-b009-5cc4a098f713", 10007140, 1000107, 1616938837, "2021-10-29 22:05:14.365038-07")
    };
    public final Entry[] orders_entries = {
        new Entry("65f885d6-cba5-43c2-a272-a2c0a661f9a2", 0, 7235, 1, 17.35, 0, "2021-10-29 22:05:14.365038-07"),
        new Entry("65f885d6-cba5-43c2-a272-a2c0a661f9a2", 1, 8757, 2, 57.50, 11.50, "2021-10-29 22:05:14.365038-07"),
        new Entry("88cd1b18-8488-4e67-aaab-1f0decbbe832", 0, 3571, 1, 41.95, 0, "2021-10-29 22:05:14.365038-07"),
        new Entry("28c1278d-5df9-4f28-b033-3a850f53bafa", 0, 7552, 3, 25.50, 15.00, "2021-10-29 22:05:14.365038-07"),
        new Entry("28c1278d-5df9-4f28-b033-3a850f53bafa", 1, 3225, 1, 105.00, 0, "2021-10-29 22:05:14.365038-07"),
        new Entry("3037f704-715f-4330-b009-5cc4a098f713", 0, 1332, 8, 8.49, 0, "2021-10-29 22:05:14.365038-07"),
        new Entry("3037f704-715f-4330-b009-5cc4a098f713", 1, 3571, 1, 41.95, 5.00, "2021-10-29 22:05:14.365038-07")

    };
  }
  @Value
  public static class Entry {
    public final String uuid;
    public final int index;
    public final int productid;
    public final int quantity;
    public final double unit_price;
    public final double discount;
    public final String __ingesttime;
  }

  @Value
  public static class Order {
    public final String uuid;
    public final int id;
    public final int customerid;
    public final int time;
    public final String __ingesttime;
  }
}