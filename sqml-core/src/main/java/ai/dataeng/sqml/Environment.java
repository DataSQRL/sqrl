package ai.dataeng.sqml;

import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.config.util.NamedIdentifier;
import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.io.sinks.DataSinkRegistration;
import ai.dataeng.sqml.io.sinks.registry.DataSinkRegistry;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;
import ai.dataeng.sqml.parser.ScriptParser;
import ai.dataeng.sqml.parser.ScriptProcessor;
import ai.dataeng.sqml.parser.validator.Validator;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.parser.Script;
import ai.dataeng.sqml.parser.operator.ImportResolver;
import ai.dataeng.sqml.planner.Planner3;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.config.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Environment implements Closeable {

  private final SqrlSettings settings;

  private final MetadataStore metadataStore;
  private final StreamEngine streamEngine;

  private final DatasetRegistry datasetRegistry;
  private final DataSinkRegistry dataSinkRegistry;
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
    dataSinkRegistry = new DataSinkRegistry(settings.getDataSinkRegistryPersistenceProvider().createRegistryPersistence(metadataStore));
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

    Validator validator = settings.getValidatorProvider().getValidator();
    ErrorCollector errors = validator.validate(scriptNode);

    if (errors.isFatal()) {
      throw new Exception("Could not compile script.");
    }
    HeuristicPlannerProvider planner =
        settings.getHeuristicPlannerProvider();
    ScriptProcessor processor = settings.getScriptProcessorProvider().createScriptProcessor(
        importResolver, planner, settings.getNamespace());
    Namespace namespace = processor.process(scriptNode);

//    QueryAnalyzer.addDevModeQueries(logicalPlan);
    Preconditions.checkArgument(!errors.isFatal());

//    LogicalPlanOptimizer.Result optimized = new SimpleOptimizer()
//        .optimize(logicalPlan);

    JDBCConnectionProvider jdbc = settings.getJdbcConfiguration().getDatabase(submission.getId().getId());


//    SQLGenerator.Result sql = settings.getSqlGeneratorProvider()
//        .create(jdbc)
//        .generateDatabase(optimized);
//    sql.executeDMLs();

//    List<MaterializeSource> sources = optimized.getReadLogicalPlan();
//
//    SqrlCodeRegistryBuilder codeRegistryBuilder = new SqrlCodeRegistryBuilder();
//    GraphQLCodeRegistry registry = codeRegistryBuilder.build(settings.getSqlClientProvider(), sources);
//
//    StreamEngine.Job job = settings.getStreamGeneratorProvider()
//        .create(streamEngine,jdbc)
//        .generateStream(optimized, sql.getSinkMapper());
//
//    job.execute(submission.getId().getId());
//    submission.setExecutionId(job.getId());

    return new Script(namespace, null);
  }

  public DatasetRegistry getDatasetRegistry() {
    return datasetRegistry;
  }

  public DataSinkRegistry getDataSinkRegistry() {
    return dataSinkRegistry;
  }

  @Override
  public void close() {
    //Clean up stuff
    metadataStore.close();
  }
}
