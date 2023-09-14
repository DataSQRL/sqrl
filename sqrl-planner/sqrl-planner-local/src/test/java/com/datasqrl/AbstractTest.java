package com.datasqrl;

import static com.datasqrl.io.tables.TableConfig.CONNECTOR_KEY;

import com.datasqrl.FlinkExecutablePlan.FlinkQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkStreamQuery;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.schema.ScriptPlanner;
import com.datasqrl.plan.ScriptValidator;
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
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.functions.DefaultFunctions;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.APIConnectorManagerImpl;
import com.datasqrl.graphql.inference.GraphQLMutationExtraction;
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
import com.datasqrl.kafka.KafkaLogEngineFactory;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderComposite;
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
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlStatement;
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
    DefaultFunctions functions = new DefaultFunctions(new FlinkConverter(framework.getQueryPlanner().getRexBuilder(),
        framework.getTypeFactory()));
    functions.getDefaultFunctions()
        .forEach((key, value) -> framework.getSqrlOperatorTable().addFunction(key, value));

    ResourceResolver resolver = new FileResourceResolver(
        Path.of("/Users/henneberger/sqrl/sqrl-examples/conference"));
    ObjectLoader objectLoader = new ObjectLoaderImpl(resolver, ErrorCollector.root(), new CalciteTableFactory(framework, NameCanonicalizer.SYSTEM));
    String gqlSchema = "scalar DateTime\n"
        + "\n"
        + "type Query {\n"
        + "    PersonalizedEvents( userid: String!, tolerance: Float!, afterTime: DateTime!): [PersonalizedEvents!]\n"
        + "    EventsAfterTime(afterTime: DateTime!): [Events!]!\n"
        + "    EventSearch( query: String!, afterTime: DateTime!): [EventSearchResult!]!\n"
        + "}\n"
        + "\n"
        + "interface AbstractEvents {\n"
        + "    String : Int!\n"
        + "    time : String!\n"
        + "    location: String!\n"
        + "    title : String!\n"
        + "    description : String!\n"
        + "    name : String!\n"
//        + "    likeCount : LikeCount\n"
        + "}\n"
        + "\n"
        + "type LikeCount {\n"
        + "    num : Int\n"
        + "}\n"
        + "\n"
        + "type EventSearchResult {\n"
        + "    id : String!\n"
        + "    time : String!\n"
        + "    location: String!\n"
        + "    title : String!\n"
        + "    description : String!\n"
        + "    name : String!\n"
//        + "    likeCount : LikeCount\n"
        + "}\n"
        + "type Events implements AbstractEvents{\n"
        + "    id : String!\n"
        + "    time : String!\n"
        + "    location: String!\n"
        + "    title : String!\n"
        + "    description : String!\n"
        + "    name : String!\n"
//        + "    likeCount : LikeCount\n"
        + "}\n"
        + "\n"
        + "type PersonalizedEvents implements AbstractEvents{\n"
        + "    id : String!\n"
        + "    time : String!\n"
        + "    location: String!\n"
        + "    title : String!\n"
        + "    description : String!\n"
        + "    name : String!\n"
//        + "    likeCount : LikeCount\n"
        + "    liked: Int!\n"
        + "    score: Float!\n"
        + "}\n"
        + "\n"
        + "type Subscription {\n"
        + "    EventNotification: Events\n"
        + "}\n"
        + "\n"
        + "\n"
        + "type Mutation {\n"
        + "    EventUpdate(event: EventUpdate!): EventUpdated\n"
        + "    Likes(liked: LikedInput!): CreatedLiked\n"
        + "    ReportEvent(report: EventReport!): EventReported\n"
        + "    AddInterest(interest: AddInterest!): InterestAdded\n"
        + "    EventRemoval(removal: EventRemoval!): EventRemoved\n"
        + "}\n"
        + "\n"
        + "input EventUpdate {\n"
        + "    userid: String!\n"
        + "    id: String!\n"
        + "    name: String!\n"
        + "    email: String!\n"
        + "    location: String!\n"
        + "    time: DateTime!\n"
        + "    title: String!\n"
        + "    description: String!\n"
        + "    secret: String!\n"
        + "}\n"
        + "\n"
        + "type EventUpdated {\n"
        + "    _source_time: String!\n"
        + "    userid: String!\n"
        + "}\n"
        + "\n"
        + "input EventRemoval {\n"
        + "    eventId: String!\n"
        + "    auth_token: String!\n"
        + "}\n"
        + "\n"
        + "type EventRemoved {\n"
        + "    _source_time: String!\n"
        + "}\n"
        + "\n"
        + "input AddInterest {\n"
        + "    text: String!\n"
        + "    userid: String!\n"
        + "}\n"
        + "\n"
        + "type InterestAdded {\n"
        + "    _source_time: String!\n"
        + "    userid: String!\n"
        + "}\n"
        + "\n"
        + "input EventReport {\n"
        + "    eventId: String!\n"
        + "    userid: String!\n"
        + "}\n"
        + "\n"
        + "type EventReported {\n"
        + "    _source_time: String!\n"
        + "    userid: String!\n"
        + "}\n"
        + "\n"
        + "input LikedInput {\n"
        + "    eventId: String!\n"
        + "    userid: String!\n"
        + "    liked: Int!\n"
        + "}\n"
        + "\n"
        + "type CreatedLiked {\n"
        + "    _source_time: String!\n"
        + "    userid: String!\n"
        + "}";
    ErrorCollector errors = ErrorCollector.root();

    SqrlConfig config = SqrlConfigCommons.create(errors);
    config.getSubConfig("streams")
        .setProperty(EngineFactory.ENGINE_NAME_KEY, FlinkEngineFactory.ENGINE_NAME);
    config.getSubConfig("log")
        .setProperty(EngineFactory.ENGINE_NAME_KEY, KafkaLogEngineFactory.ENGINE_NAME);
    SqrlConfig log = config.getSubConfig("log")
        .getSubConfig(CONNECTOR_KEY);
    log.setProperty("name", "kafka");
    config.getSubConfig("log")
        .setProperty("type", "source_and_sink");
    config.getSubConfig("log")
        .setProperty("schema", "flexible");
    config.getSubConfig("log").getSubConfig("format")
        .setProperty("name", "json");
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

    APISource apiSchema =
        new APISource(Name.system("myAPI"), gqlSchema);
    ModuleLoader moduleLoader = new ModuleLoaderImpl(objectLoader);


    APIConnectorManager apiManager = new APIConnectorManagerImpl(new CalciteTableFactory(framework, NameCanonicalizer.SYSTEM),
        pipeline, errors, moduleLoader, framework.getTypeFactory());

    //Magic order: must call before getting
    GraphQLMutationExtraction extraction = new GraphQLMutationExtraction(framework.getTypeFactory(),
        NameCanonicalizer.SYSTEM);
    extraction.analyze(apiSchema, apiManager);

    ModuleLoader moduleLoader1 = apiManager.getAsModuleLoader();
    moduleLoader = new ModuleLoaderComposite(List.of(moduleLoader, moduleLoader1));

    SqrlTableFactory tableFactory = new SqrlPlanningTableFactory(framework, NameCanonicalizer.SYSTEM);

    String script = "IMPORT mysourcepackage.Events AS ConferenceEvents TIMESTAMP last_updated AS timestamp;\n"
        + "IMPORT mysourcepackage.AuthTokens;\n"
        + "IMPORT mysourcepackage.EmailTemplates\n"
        + "\n"
        + "IMPORT myAPI.EventUpdate TIMESTAMP _source_time AS timestamp;\n"
        + "IMPORT myAPI.Likes;\n"
        + "IMPORT myAPI.ReportEvent;\n"
        + "IMPORT myAPI.AddInterest;\n"
        + "IMPORT myAPI.EventRemoval;\n"
        + "\n"
        + "IMPORT string.*;\n"
        + "IMPORT text.*;\n"
        + "IMPORT secure.randomID;\n"
        + "\n"
        + "EmailTemplates := DISTINCT EmailTemplates ON id ORDER BY last_updated DESC;\n"
        + "AuthTokens := DISTINCT AuthTokens ON id ORDER BY last_updated DESC;\n"
        + "\n"
        + "EventPosts := SELECT * FROM EventUpdate WHERE id IS NOT NULL;\n"
        + "\n"
        + "EventPosts.id := randomID(12);\n"
        + "EventPosts.secret := randomID(10);\n"
        + "\n"
        + "EventPostEmail := SELECT e.email as toEmail, t.fromEmail, t.title,\n"
        + "                         format(t.textBody, e.name, e.id, e.secret) as textBody\n"
        + "                  FROM EventPosts e TEMPORAL JOIN EmailTemplates t ON t.id = 'confirmpost';\n"
        + "\n"
        + "EXPORT EventPostEmail TO print.eventpostemail; --sink to queue and email sending\n"
        + "\n"
        + "EventSecrets := DISTINCT EventPosts ON id ORDER BY timestamp DESC;\n"
        + "\n"
        + "VerifiedEventUpdate := SELECT u.* FROM EventUpdate u TEMPORAL JOIN EventSecrets s ON u.id = s.id AND u.secret = s.secret;\n"
        + "\n"
        + "AuthorizedEventRemoval := SELECT r.* FROM EventRemoval r TEMPORAL JOIN AuthTokens a ON a.id = 'removal' AND a.value = r.auth_token;\n"
        + "\n"
        + "FilteredEventUpdate := SELECT v.*, coalesce(r._uuid, '') AS removalId, ((NOT bannedWordsFilter(v.name))\n"
        + "                            OR (NOT bannedWordsFilter(v.description)) OR (NOT bannedWordsFilter(v.location))) AS removed\n"
        + "                       FROM VerifiedEventUpdate v LEFT INTERVAL JOIN AuthorizedEventRemoval r ON v.id = r.eventId AND v.timestamp < r._source_time;\n"
        + "\n"
        + "/* requires string aggregated concatenation\n"
        + "ConferenceEvents.speakerSummary := SELECT concatAgg( s.name + '(' + s.title + ' at ' + s.company + ')', ', ') as name\n"
        + "                                 FROM @.speakers s;\n"
        + " */\n"
        + "/* Replace with above */\n"
        + "ConferenceEvents.speakerSummary := SELECT CONCAT('speakerlength', (sum(length(s.name) + length(s.title) + length(s.company)))) as name\n"
        + "                                   FROM @.speakers s;\n"
        + "\n"
        + "ConferenceEventsFlat := SELECT e._uuid, e.timestamp, e.id, e.time, e.title, e.description, e.location, s.name as name\n"
        + "                        FROM ConferenceEvents e JOIN e.speakerSummary s;\n"
        + "\n"
        + "Events := SELECT _uuid, removalId, timestamp, id, time, title, description, name, location, removed FROM FilteredEventUpdate\n"
        + "          UNION ALL\n"
        + "          SELECT _uuid, '' as removalId, timestamp, id, time, title, description, name, location, FALSE as removed FROM ConferenceEventsFlat;\n"
        + "\n"
        + "EventNotification := SELECT * FROM FilteredEventUpdate WHERE NOT removed;\n"
        + "\n"
        + "--Events.embedding := embed_text(title + description);\n"
        + "Events.embedding := length(description); -- replace by vector embedding\n"
        + "\n"
        + "Events := DISTINCT Events ON id ORDER BY timestamp DESC;\n"
        + "\n"
        + "--Interests.embedding := embed_text(text);\n"
        + "AddInterest.embedding := length(text); -- replace by vector embedding\n"
        + "\n"
        + "UserInterests := SELECT userid, avg(embedding) as interestVector FROM AddInterest GROUP BY userid;\n"
        + "\n"
        + "UserLikes := DISTINCT Likes ON userid, eventId ORDER BY _source_time DESC;\n"
        + "\n"
        + "\n"
        + "EventLikeCount := SELECT eventid, sum(liked) as num FROM UserLikes l GROUP BY eventid;\n"
        + "Events.likeCount := JOIN EventLikeCount l ON @.id = l.eventid;\n"
        + "\n"
        + "EventsAfterTime(@afterTime: DateTime) := SELECT * FROM Events WHERE time > @afterTime AND NOT removed\n"
        + "                                                             ORDER BY time ASC;\n"
        + "\n"
        + "EventSearch(@query: String, @afterTime: DateTime) := SELECT * FROM Events WHERE\n"
        + "                        time >= @afterTime AND textsearch(@query, title, description) > 0\n"
        + "                        AND NOT removed\n"
        + "                        ORDER BY textsearch(@query, title, description) DESC;\n"
        + "\n"
        + "-- Events that the user liked or that are similar to users interests (requires vector search)\n"
        + "\n"
        + "PersonalizedEvents(@userid: String, @tolerance: Float, @afterTime: DateTime) :=\n"
        + "SELECT e.*, l.liked,\n"
        + "       coalesce(l.eventId, '') as shouldRemove1,\n"
        + "       i.interestVector / e.embedding as score\n"
        + "--       cosineSimilarity(i.interestVector, e.embedding) as score\n"
        + "FROM Events e\n"
        + "    LEFT JOIN UserLikes l ON e.id = l.eventId AND l.userid = @userid\n"
        + "    LEFT JOIN UserInterests i ON i.userid = @userid\n"
        + "    WHERE e.time > @afterTime AND NOT removed\n"
        + "          AND (l.liked = 1 OR (i.interestVector IS NOT NULL AND\n"
        + "                               i.interestVector / CAST(e.embedding AS Float) >= @tolerance))\n"
        + "--                               cosineSimilarity(i.interestVector, e.embedding) >= @tolerance))\n"
        + "    ORDER BY e.time ASC\n"
        + "\n"
        + "--PersonalizedEvents.likeCount := JOIN EventLikeCount l ON @.id = l.eventid;\n"
        + "\n"
        + "\n"
        + "FlaggedEventEmail := SELECT t.toEmail, t.fromEmail, t.title,\n"
        + "                         format(t.textBody, r.eventid, e.title, e.description, e.name) as textBody\n"
        + "                  FROM ReportEvent r\n"
        + "                  TEMPORAL JOIN Events e ON r.eventid = e.id\n"
        + "                  TEMPORAL JOIN EmailTemplates t ON t.id = 'eventflag';\n"
        + "\n"
        + "EXPORT FlaggedEventEmail TO print.flaggedEventEmail;";

    ScriptValidator validator = new ScriptValidator(framework, framework.getQueryPlanner(), moduleLoader,
        errors.withSchema("<script>", script), new SqlNameUtil(NameCanonicalizer.SYSTEM));

    ScriptPlanner planner = new ScriptPlanner(
        framework.getQueryPlanner(), validator,
        new SqrlPlanningTableFactory(framework, NameCanonicalizer.SYSTEM), framework,
        new SqlNameUtil(NameCanonicalizer.SYSTEM), moduleLoader, errors);

    ScriptNode node = (ScriptNode)framework.getQueryPlanner().parse(Dialect.SQRL, script);
    for (SqlNode statement : node.getStatements()) {
      validator.validateStatement((SqrlStatement) statement);
      planner.plan(statement);
    }

//    GraphQLSchema gqlSchema = new SchemaGenerator().generate(framework.getSchema());
//    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
//        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
//        .includeDirectives(false);
//    String schema = new SchemaPrinter(opts).print(gqlSchema);


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
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(framework, errorSink.getErrorSink());

    PhysicalPlan plan = physicalPlanner.plan(dagPlan);

    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(plan.getDatabaseQueries(),
        framework.getQueryPlanner());

    root.accept(replaceGraphqlQueries, null);

    snapshot.addContent(script+ "\n", "Script");
    snapshot.addContent(gqlSchema+ "\n", "Schema");
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
