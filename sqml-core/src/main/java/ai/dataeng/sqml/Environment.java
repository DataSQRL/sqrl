package ai.dataeng.sqml;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.api.graphql.SqrlCodeRegistryBuilder;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
import ai.dataeng.sqml.config.util.NamedIdentifier;
import ai.dataeng.sqml.execution.StreamEngine;
import ai.dataeng.sqml.type.schema.SchemaConversionError;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.execution.sql.SQLGenerator;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;
import ai.dataeng.sqml.parser.ScriptParser;
import ai.dataeng.sqml.parser.processor.ScriptProcessor;
import ai.dataeng.sqml.parser.validator.Validator;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.Script;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import ai.dataeng.sqml.planner.operator.QueryAnalyzer;
import ai.dataeng.sqml.planner.optimize.LogicalPlanOptimizer;
import ai.dataeng.sqml.planner.optimize.MaterializeSource;
import ai.dataeng.sqml.planner.optimize.SimpleOptimizer;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import com.google.common.base.Preconditions;
import graphql.schema.GraphQLCodeRegistry;

import java.io.Closeable;
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
  private final EnvironmentPersistence persistence;

  private Environment(SqrlSettings settings) {
    this.settings = settings;
    JDBCConnectionProvider jdbc = settings.getJdbcConfiguration().getDatabase(
            settings.getEnvironmentConfiguration().getMetastore().getDatabase());
    metadataStore = settings.getMetadataStoreProvider().openStore(jdbc);
    streamEngine = settings.getStreamEngineProvider().create();
    persistence = settings.getEnvironmentPersistenceProvider().createEnvironmentPersistence(metadataStore);

    SourceTableMonitor monitor = settings.getSourceTableMonitorProvider().create(streamEngine,
            settings.getStreamMonitorProvider().create(streamEngine,jdbc,
                    settings.getMetadataStoreProvider(),settings.getDatasetRegistryPersistenceProvider()));
    datasetRegistry = new DatasetRegistry(settings.getDatasetRegistryPersistenceProvider()
            .createRegistryPersistence(metadataStore),monitor);
  }

  public static Environment create(SqrlSettings settings) {
    return new Environment(settings);
  }

  public ScriptDeployment.Result deployScript(@NonNull ScriptBundle.Config scriptConfig,
                                              @NonNull ProcessBundle<ConfigurationError> errors) {
    ScriptBundle bundle = scriptConfig.initialize(errors);
    if (bundle==null) return null;
    ScriptDeployment submission = ScriptDeployment.of(bundle);
    //TODO: Need to collect errors from compile() and return them
    try {
      compile(submission);
    } catch (Exception e) {
      errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,bundle.getName().getDisplay(),
              "Encountered error while compiling script: %s",e));
      return null;
    }
    persistence.saveDeployment(submission);
    return submission.getStatusResult(streamEngine);
  }

  public Optional<ScriptDeployment.Result> getDeployment(@NonNull NamedIdentifier submissionId) {
    ScriptDeployment submission = persistence.getSubmissionById(submissionId);
    if (submission==null) return Optional.empty();
    else return Optional.of(submission.getStatusResult(streamEngine));
  }

  public List<ScriptDeployment.Result> getActiveDeployments() {
    return persistence.getAllDeployments().filter(ScriptDeployment::isActive)
            .map(s -> s.getStatusResult(streamEngine)).collect(Collectors.toList());
  }

  public Script compile(ScriptDeployment submission) throws Exception {
    ScriptBundle bundle = submission.getBundle();
    SqrlScript mainScript = bundle.getMainScript();

    //Instantiate import resolver and register user schema
    ImportResolver importResolver = settings.getImportManagerProvider().createImportManager(datasetRegistry);
    ProcessBundle<SchemaConversionError> importErrors = importResolver.getImportManager()
            .registerUserSchema(mainScript.getSchema());
    Preconditions.checkArgument(!importErrors.isFatal(),
            importErrors);

    ScriptParser scriptParser = settings.getScriptParserProvider().createScriptParser();
    ScriptNode scriptNode = scriptParser.parse(mainScript);

    Validator validator = settings.getValidatorProvider().getValidator();
    ProcessBundle<ProcessMessage> errors = validator.validate(scriptNode);

    if (errors.isFatal()) {
      throw new Exception("Could not compile script.");
    }
    HeuristicPlannerProvider planner =
        settings.getHeuristicPlannerProvider();
    ScriptProcessor processor = settings.getScriptProcessorProvider().createScriptProcessor(
        settings.getImportProcessorProvider().createImportProcessor(importResolver, planner),
        settings.getQueryProcessorProvider().createQueryProcessor(planner),
        settings.getExpressionProcessorProvider().createExpressionProcessor(planner),
        settings.getJoinProcessorProvider().createJoinProcessor(),
        settings.getDistinctProcessorProvider().createDistinctProcessor(),
        settings.getSubscriptionProcessorProvider().createSubscriptionProcessor(),
        settings.getNamespace());

    Namespace namespace = processor.process(scriptNode);

    LogicalPlanImpl logicalPlan = namespace.getLogicalPlan();

    QueryAnalyzer.addDevModeQueries(logicalPlan);
    Preconditions.checkArgument(!errors.isFatal());

    LogicalPlanOptimizer.Result optimized = new SimpleOptimizer()
        .optimize(logicalPlan);

    JDBCConnectionProvider jdbc = settings.getJdbcConfiguration().getDatabase(submission.getId().getId());
    SQLGenerator.Result sql = settings.getSqlGeneratorProvider()
        .create(jdbc)
        .generateDatabase(optimized);
    sql.executeDMLs();

    List<MaterializeSource> sources = optimized.getReadLogicalPlan();

    SqrlCodeRegistryBuilder codeRegistryBuilder = new SqrlCodeRegistryBuilder();
    GraphQLCodeRegistry registry = codeRegistryBuilder.build(settings.getSqlClientProvider(), sources);

    StreamEngine.Job job = settings.getStreamGeneratorProvider()
        .create(streamEngine,jdbc)
        .generateStream(optimized, sql.getSinkMapper());

    job.execute(submission.getId().getId());
    submission.setExecutionId(job.getId());

    return new Script(namespace, registry);
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
