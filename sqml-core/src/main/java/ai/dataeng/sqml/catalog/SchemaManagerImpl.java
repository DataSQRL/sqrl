package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.SneakyThrows;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

public class SchemaManagerImpl implements SchemaManager {
//
//  public void addImport(NamePath namePath) {
//    importManager.resolve(namePath);
//  }
//
//  //TODO: MOVE OUT
//  public SqrlTable2 addQuery(NamePath namePath, String sql) {
//    RelNode node = parse(sql);
//    SqrlResolvedSchema schema = schemaResolver.resolve(node);
//
//    return new SqrlTable2(namePath, schema);
//  }

  @SneakyThrows
  public RelNode parse(String query) {
    SchemaPlus schema = Frameworks.createRootSchema(false);
//    schema.add("default_schema", new ReflectiveSchema(new C360()));

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
        .programs(Programs.ofRules())
        .build();

    Planner planner = Frameworks.getPlanner(config);

    SqlNode parse = planner.parse(query);

    SqlNode validate = planner.validate(parse);
    RelRoot planRoot = planner.rel(validate);
    return planRoot.project();
  }
}
