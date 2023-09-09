package com.datasqrl;

import com.datasqrl.FlinkExecutablePlan.FlinkQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkStreamQuery;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.schema.ScriptExecutor;
import com.datasqrl.calcite.validator.ScriptValidator;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.generate.SchemaGenerator;
import com.datasqrl.graphql.inference.SchemaBuilder;
import com.datasqrl.graphql.inference.SchemaInference;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import com.datasqrl.graphql.server.Model.JdbcQuery;
import com.datasqrl.graphql.server.Model.QueryBase;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.util.ReplaceGraphqlQueries;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.ObjectLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.datasqrl.util.SqlNameUtil;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class AbstractTest {
  Snapshot snapshot;
  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @Test
  public void test() {
    //Do all the things, get all the queries, execute them all
    SqrlFramework framework = new SqrlFramework(new SqrlRelMetadataProvider(),
        SqrlHintStrategyTable.getHintStrategyTable(), NameCanonicalizer.SYSTEM);

    ResourceResolver resolver = new FileResourceResolver(
        Path.of("/Users/henneberger/sqrl/sqrl-examples/starwars"));
    ObjectLoader objectLoader = new ObjectLoaderImpl(resolver, ErrorCollector.root(), new CalciteTableFactory(framework, NameCanonicalizer.SYSTEM));
    ModuleLoader moduleLoader = new ModuleLoaderImpl(objectLoader);

    SqrlTableFactory tableFactory = new SqrlPlanningTableFactory(framework, NameCanonicalizer.SYSTEM);

    ScriptExecutor executor = new ScriptExecutor(tableFactory, framework, new SqlNameUtil(NameCanonicalizer.SYSTEM),
        moduleLoader, ErrorCollector.root());

    ScriptValidator validator = new ScriptValidator(framework, moduleLoader);
    String script = "IMPORT starwars.Human;\n"
        + "State := DISTINCT Human ON name ORDER BY _ingest_time;"
        + "NEW := SELECT h.nAmE AS name, h2.NaMe AS name0 FROM Human h TEMPORAL JOIN State h2 ON h.name = h2.name;";
    validator.validate(script);

    ScriptNode node = (ScriptNode)framework.getQueryPlanner().parse(Dialect.SQRL, script);
    for (SqlNode statement : node.getStatements()) {
      RelNode relNode = framework.getQueryPlanner().plan(Dialect.SQRL, statement);
      executor.apply(relNode);
    }

    ErrorCollector errors = ErrorCollector.root();

    SqrlConfig config = SqrlConfigCommons.create(errors);
    config.getSubConfig("streams")
        .setProperty(EngineFactory.ENGINE_NAME_KEY, FlinkEngineFactory.ENGINE_NAME);
    config.getSubConfig("database")
        .setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
    config.getSubConfig("database").setProperties(JdbcDataSystemConnector.builder()
            .url("jdbc:postgresql://database:5432/datasqrl")
            .driver("org.postgresql.Driver")
            .dialect("postgres")
            .database("datasqrl")
            .user("postgres")
            .password("postgres")
            .host("database")
            .port(5432)
            .build());
    PipelineFactory pipelineFactory = new PipelineFactory(config);
    ExecutionPipeline pipeline = pipelineFactory.createPipeline();

    GraphQLSchema gqlSchema = new SchemaGenerator().generate(framework.getSchema());
    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);
    String schema = new SchemaPrinter(opts).print(gqlSchema);

    APISource apiSchema =
        new APISource(Name.system("schema"), schema);

    APIConnectorManager apiManager = new APIConnectorManagerImpl(new CalciteTableFactory(framework, NameCanonicalizer.SYSTEM),
        pipeline, errors, moduleLoader, framework.getTypeFactory());

    InferredSchema inferredSchema = new SchemaInference(
        framework,
        apiSchema.getName().getDisplay(),
        moduleLoader,
        apiSchema,
        framework.getSchema(),
        framework.getQueryPlanner().getRelBuilder(),
        apiManager)
        .accept();

    SchemaBuilder schemaBuilder = new SchemaBuilder(framework, apiSchema,
        framework.getSchema(),
        framework.getQueryPlanner().getRelBuilder(),
        new SqrlQueryPlanner(framework, new CalciteTableFactory(framework, NameCanonicalizer.SYSTEM)),
        framework.getSqrlOperatorTable(),
        apiManager);

    RootGraphqlModel root = inferredSchema.accept(schemaBuilder,
        null);

    DAGPlanner dagPlanner = new DAGPlanner(framework, pipeline, Debugger.NONE, errors);

    PhysicalDAGPlan dagPlan = dagPlanner.plan(framework.getSchema(), apiManager,
        framework.getSchema().getExports(),
        Set.of(), Map.of(), root);

    ErrorSink errorSink = new ErrorSink(LoaderUtil.loadSink( NamePath.of("print","errors"),
        errors, moduleLoader));
    PhysicalPlanner planner = new PhysicalPlanner(framework, errorSink.getErrorSink());

    PhysicalPlan plan = planner.plan(dagPlan);

    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(plan.getDatabaseQueries(),
        framework.getQueryPlanner());

    root.accept(replaceGraphqlQueries, null);

    snapshot.addContent(script+ "\n", "Script");
    snapshot.addContent(schema+ "\n", "Schema");
    snapshot.addContent("\n", "DDL");

    plan.getPlans(JDBCPhysicalPlan.class).findAny().get().getDdlStatements().stream()
        .forEach(ddl->snapshot.addContent(ddl.toSql()));

    root.getCoords().stream()
        .filter(f->f instanceof ArgumentLookupCoords)
        .map(f->(ArgumentLookupCoords)f)
        .forEach(c->
            c.getMatchs().stream().forEach(f->snapshot.addContent(
                c.getParentType()+":"+c.getFieldName() + "" +  printQuery(f.getQuery()) + "\n")));

    snapshot.addContent("\n", "Queries");

    List<FlinkQuery> queries = plan.getPlans(FlinkStreamPhysicalPlan.class).findAny().get()
        .getExecutablePlan()
        .getBase().getQueries();

    for (FlinkQuery query : queries) {
      if (query instanceof FlinkSqlQuery) {
        FlinkSqlQuery query1 = (FlinkSqlQuery)query;
        snapshot.addContent(query1.getName() + " = " + query1.getQuery()+ "\n");
      } else {
        FlinkStreamQuery query1 = (FlinkStreamQuery)query;
        snapshot.addContent(query1.getName() + " = " + query1.getStateChangeType() + " : " + query1.getFromTable()+ "\n");
      }
    }

    snapshot.createOrValidate();
//    FlinkBase base = plan.getPlans(FlinkStreamPhysicalPlan.class).findAny().get()
//        .getExecutablePlan().getBase();
//    FlinkEnvironmentBuilder builder = new FlinkEnvironmentBuilder(errors);
//    StatementSet statementSet = builder.visitBase(base, null);
//    statementSet.execute().print();

    System.out.println();
  }

  private String printQuery(QueryBase query) {
    StringBuilder builder = new StringBuilder();
    if (query instanceof JdbcQuery) {
      JdbcQuery q = (JdbcQuery)query;
      StringJoiner joiner = new StringJoiner(", ");
      for (JdbcParameterHandler handler : q.getParameters()) {
        if (handler instanceof ArgumentParameter) {
          joiner.add(((ArgumentParameter)handler).getPath());
        } else if (handler instanceof SourceParameter)  {
          joiner.add("(env)"+((SourceParameter)handler).getKey());
        }
      }

      builder.append("(")
        .append(joiner)
        .append(")  =  ")
          .append(q.getSql());
    } else {
      throw new RuntimeException("Todo");
    }

    return builder.toString();
  }
}
