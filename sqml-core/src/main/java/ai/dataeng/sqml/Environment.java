package ai.dataeng.sqml;

import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration.Dialect;
import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.config.util.NamedIdentifier;
import ai.dataeng.sqml.execution.FlinkPipelineGenerator;
import ai.dataeng.sqml.execution.GraphqlGenerator;
import ai.dataeng.sqml.execution.SqlGenerator;
import ai.dataeng.sqml.execution.SqrlExecutor;
import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;
import ai.dataeng.sqml.parser.ScriptParser;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ShadowingContainer;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.parser.sqrl.analyzer.Analyzer;
import ai.dataeng.sqml.parser.sqrl.calcite.CalcitePlanner;
import ai.dataeng.sqml.parser.sqrl.schema.TableFactory;
import ai.dataeng.sqml.planner.SqrlPlanner;
import ai.dataeng.sqml.planner.nodes.LogicalFlinkSink;
import ai.dataeng.sqml.planner.nodes.LogicalPgSink;
import ai.dataeng.sqml.parser.Script;
import ai.dataeng.sqml.parser.operator.ImportResolver;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.config.error.ErrorCollector;
import com.google.common.base.Preconditions;
import graphql.GraphQL;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

@Slf4j
public class Environment implements Closeable {

  private final SqrlSettings settings;

  private final MetadataStore metadataStore;
  private final StreamEngine streamEngine;

  private final DatasetRegistry datasetRegistry;
  private final EnvironmentPersistence persistence;

  private Environment(SqrlSettings settings) {
    this.settings = settings;
    JDBCConnectionProvider jdbc = settings.getJdbcConfiguration().getDatabase(
            settings.getEnvironmentConfiguration().getMetastore().getDatabase());
    metadataStore = settings.getMetadataStoreProvider().openStore(jdbc, settings.getSerializerProvider());
    streamEngine = settings.getStreamEngineProvider().create();
    persistence = settings.getEnvironmentPersistenceProvider().createEnvironmentPersistence(metadataStore);

    SourceTableMonitor monitor = settings.getSourceTableMonitorProvider().create(streamEngine,
            settings.getStreamMonitorProvider().create(streamEngine,jdbc,
                    settings.getMetadataStoreProvider(), settings.getSerializerProvider(),
                    settings.getDatasetRegistryPersistenceProvider()));
    datasetRegistry = new DatasetRegistry(settings.getDatasetRegistryPersistenceProvider()
            .createRegistryPersistence(metadataStore),monitor);
  }

  public static Environment create(SqrlSettings settings) {
    return new Environment(settings);
  }

  public ScriptDeployment.Result deployScript(@NonNull ScriptBundle.Config scriptConfig,
                                              @NonNull ErrorCollector errors) {
    ScriptBundle bundle = scriptConfig.initialize(errors);
    if (bundle==null) return null;
    ScriptDeployment deployment = ScriptDeployment.of(bundle);
    //TODO: Need to collect errors from compile() and return them in compilation object
    Instant compileStart = Instant.now();
//    try {
//      compile(deployment);
//    } catch (Exception e) {
//      errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,bundle.getName().getDisplay(),
//              "Encountered error while compiling script: %s",e));
//      return null;
//    }
    //TODO: Need to put the actual compilation results in here
    CompilationResult compilationResult = CompilationResult.generateDefault(bundle,
            Duration.between(compileStart,Instant.now()).toMillis());
    persistence.saveDeployment(deployment);
    return deployment.getStatusResult(streamEngine, Optional.of(compilationResult));
  }

  public Optional<ScriptDeployment.Result> getDeployment(@NonNull NamedIdentifier submissionId) {
    ScriptDeployment submission = persistence.getSubmissionById(submissionId);
    if (submission==null) return Optional.empty();
    else return Optional.of(submission.getStatusResult(streamEngine, Optional.empty()));
  }

  public List<ScriptDeployment.Result> getActiveDeployments() {
    return persistence.getAllDeployments().filter(ScriptDeployment::isActive)
            .map(s -> s.getStatusResult(streamEngine, Optional.empty())).collect(Collectors.toList());
  }

  public Script compile(ScriptDeployment submission) throws Exception {
    ScriptBundle bundle = submission.getBundle();
    SqrlScript mainScript = bundle.getMainScript();

    //Instantiate import resolver and register user schema
    ImportResolver importResolver = settings.getImportManagerProvider().createImportManager(
        datasetRegistry);
    ErrorCollector importErrors = importResolver.getImportManager()
            .registerUserSchema(mainScript.getSchema());
    Preconditions.checkArgument(!importErrors.isFatal(),
            importErrors);

    ScriptParser scriptParser = settings.getScriptParserProvider().createScriptParser();
    ScriptNode scriptNode = scriptParser.parse(mainScript);

    LogicalDag dag = new LogicalDag(new ShadowingContainer<>());
    CalcitePlanner calcitePlanner = new CalcitePlanner(dag);
    Analyzer analyzer = new Analyzer(importResolver.getImportManager(), calcitePlanner, new TableFactory(calcitePlanner),
        dag);
    analyzer.analyze(scriptNode);
//
//
//    SqrlPlanner planner = new SqrlPlanner();
//    planner.setDevQueries(dag);
//
//    Pair<List<LogicalFlinkSink>, List<LogicalPgSink>> flinkSinks = planner.optimize(dag);
//
//    FlinkPipelineGenerator pipelineGenerator = new FlinkPipelineGenerator();
//    Pair<StreamStatementSet, Map<Table, TableDescriptor>> result =
//        pipelineGenerator.createFlinkPipeline(flinkSinks.getKey());
//
//    SqlGenerator sqlGenerator = new SqlGenerator(result.getRight());
//    List<String> db = sqlGenerator.generate();
//
//    JDBCConnectionProvider config = new JDBCConfiguration.Database(
//        "jdbc:postgresql://localhost/henneberger",
//        null, null, null, Dialect.POSTGRES, "henneberger"
//    );
//
//    SqrlExecutor executor = new SqrlExecutor();
//    executor.executeDml(config, db);
//    executor.executeFlink(result.getLeft());
//
//    GraphqlGenerator graphqlGenerator = new GraphqlGenerator();
//    GraphQL graphql = graphqlGenerator.graphql(dag, flinkSinks, result.getRight(), getPostgresClient());

//    JDBCConnectionProvider jdbc = settings.getJdbcConfiguration().getDatabase(submission.getId().getId());

//    return new Script(graphql);
    return null;
  }

  private SqlClientProvider getPostgresClient() {
    //TODO: this is hardcoded for now and needs to be integrated into configuration
    JDBCPool pool = JDBCPool.pool(
        Vertx.vertx(),
        new JDBCConnectOptions()
            .setJdbcUrl("jdbc:postgresql://localhost/henneberger"),
        new PoolOptions()
            .setMaxSize(1)
    );

    return () -> pool;
  }
  public DatasetRegistry getDatasetRegistry() {
    return datasetRegistry;
  }

  @Override
  public void close() {
    //Clean up stuff
    metadataStore.close();
  }
}
