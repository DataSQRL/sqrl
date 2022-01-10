package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.SimpleSqrlCalciteSchema;
import org.apache.calcite.jdbc.SqrlToCalciteTableTranslator;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.rules.RuleStub;
import org.apache.calcite.sql.RewriteTableName;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

public class HeuristicPlannerImpl implements Planner {

  @Override
  @SneakyThrows
  public PlannerResult plan(Optional<NamePath> context, Namespace namespace, String sql) {
    SqrlToCalciteTableTranslator tableTranslator = new SqrlToCalciteTableTranslator(
        context, namespace
    );

    SimpleSqrlCalciteSchema schema = new SimpleSqrlCalciteSchema(null,
        new SqrlSchema(tableTranslator),
        "");

    Config parserConfig = SqlParser.config()
        .withParserFactory(SqlParser.config().parserFactory())
        .withLex(Lex.JAVA)
        .withConformance(SqlConformanceEnum.LENIENT)
        .withIdentifierMaxLength(256);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(schema.plus())
        .parserConfig(parserConfig)
        .sqlToRelConverterConfig(SqlToRelConverter.config()
            .withRelBuilderConfigTransform(e-> {
              return RelBuilder.Config.DEFAULT.withBloat(-100)
//                  .withSimplify(true)
//                  .withSimplifyLimit(true)
//                  .withPruneInputOfAggregate(true)
//                  .withPushJoinCondition(true)
//                  .withAggregateUnique(true)
//                  .withDedupAggregateCalls(true)
                  ;
            }))
        .typeSystem(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(Programs.ofRules())
        .build();


    PlannerImpl planner = new PlannerImpl(config);
    SqlNode node = planner.parse(sql);
    RewriteTableName.rewrite(node);
    planner.validate(node);
    RelRoot root = planner.rel(node);

//    System.out.println(root.rel.getRowType());
//    System.out.println(RelToSql.convertToSql(root.rel));

    HepProgram hep = HepProgram.builder()
        .addRuleClass(RuleStub.class)
        .build();
    HepPlanner hepPlanner = new HepPlanner(hep);
    hepPlanner.addRule(new RuleStub(RuleStub.Config.DEFAULT));
    hepPlanner.setRoot(root.rel);

    RelNode optimizedNode = hepPlanner.findBestExp();
//    System.out.println(optimizedNode);

    return new PlannerResult(optimizedNode, node);
  }

}
