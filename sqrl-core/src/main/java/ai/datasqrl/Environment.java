package ai.datasqrl;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.execute.Job;
import ai.datasqrl.execute.ScriptExecutor;
import ai.datasqrl.graphql.execution.SqlClientProvider;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.util.NamedIdentifier;
import ai.datasqrl.physical.ExecutionPlan;
import ai.datasqrl.server.CompilationResult;
import ai.datasqrl.server.EnvironmentPersistence;
import ai.datasqrl.server.ScriptDeployment;
import ai.datasqrl.server.ScriptDeployment.Result;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.io.sinks.registry.DataSinkRegistry;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceTableMonitor;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
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

  public Result deployScript(@NonNull ScriptBundle.Config scriptConfig,
                                              @NonNull ErrorCollector errors) {
    ScriptBundle bundle = scriptConfig.initialize(errors);
    if (bundle==null) return null;
    //TODO: Need to collect errors from compile() and return them in compilation object
    Instant compileStart = Instant.now();
    ScriptDeployment deployment;
    try {
      ExecutionPlan plan = compile(bundle);
      ScriptExecutor executor = new ScriptExecutor(/*Settings*/);
      Job job = executor.execute(plan);
      deployment = ScriptDeployment.of(bundle);
      deployment.setExecutionId(job.getExecutionId());
    } catch (Exception e) {
      e.printStackTrace();
//      errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.SCRIPT,bundle.getName().getDisplay(),
//              "Encountered error while compiling script: %s",e));
      return null;
    }
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

  public ExecutionPlan compile(ScriptBundle bundle) throws Exception {
    BundleOptions options = BundleOptions.builder()
        .importManager(settings.getImportManagerProvider().createImportManager(datasetRegistry))
        .build();
    BundleProcessor bundleProcessor = new BundleProcessor(options);
    return bundleProcessor.processBundle(bundle);
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
