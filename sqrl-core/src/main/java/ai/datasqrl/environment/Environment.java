package ai.datasqrl.environment;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.config.EnvironmentConfiguration.MetaData;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.metadata.MetadataStore;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.provider.TableStatisticsStoreProvider;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.util.NamedIdentifier;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.ScriptExecutor;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.io.sinks.registry.DataSinkRegistry;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceTableMonitor;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.plan.BundlePlanner;
import ai.datasqrl.environment.ScriptDeployment.Result;
import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import ai.datasqrl.plan.calcite.CalciteEnvironment;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
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
    metadataStore = settings.getMetadataStoreProvider()
        .openStore(jdbc, settings.getSerializerProvider());
    streamEngine = settings.getStreamEngineProvider().create();
    persistence = settings.getEnvironmentPersistenceProvider()
        .createEnvironmentPersistence(metadataStore);

    TableStatisticsStoreProvider.Encapsulated statsStore = new TableStatisticsStoreProvider.EncapsulatedImpl(jdbc,
            settings.getMetadataStoreProvider(), settings.getSerializerProvider(), settings.getTableStatisticsStoreProvider());
    SourceTableMonitor monitor = settings.getSourceTableMonitorProvider().create(streamEngine, statsStore);
    datasetRegistry = new DatasetRegistry(settings.getDatasetRegistryPersistenceProvider()
        .createRegistryPersistence(metadataStore), monitor);
    dataSinkRegistry = new DataSinkRegistry(
        settings.getDataSinkRegistryPersistenceProvider().createRegistryPersistence(metadataStore));
  }

  public static Environment create(SqrlSettings settings) {
    return new Environment(settings);
  }

  public Result deployScript(@NonNull ScriptBundle.Config scriptConfig,
      @NonNull ErrorCollector errors) {
    ScriptBundle bundle = scriptConfig.initialize(errors);
    if (bundle == null) {
      return null;
    }
    //TODO: Need to collect errors from compile() and return them in compilation object
    Instant compileStart = Instant.now();
    ScriptDeployment deployment;
    try {
      PhysicalPlan plan = compile(bundle);
      ScriptExecutor executor = new ScriptExecutor(
          this.settings.getJdbcConfiguration().getDatabase(MetaData.DEFAULT_DATABASE));
      Job job = executor.execute(plan);
      deployment = ScriptDeployment.of(bundle);
      deployment.setExecutionId(job.getExecutionId());
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
    //TODO: Need to put the actual compilation results in here
    CompilationResult compilationResult = CompilationResult.generateDefault(bundle,
        Duration.between(compileStart, Instant.now()).toMillis());
    persistence.saveDeployment(deployment);
    return deployment.getStatusResult(streamEngine, Optional.of(compilationResult));
  }

  public Optional<ScriptDeployment.Result> getDeployment(@NonNull NamedIdentifier submissionId) {
    ScriptDeployment submission = persistence.getSubmissionById(submissionId);
    if (submission == null) {
      return Optional.empty();
    } else {
      return Optional.of(submission.getStatusResult(streamEngine, Optional.empty()));
    }
  }

  public List<ScriptDeployment.Result> getActiveDeployments() {
    return persistence.getAllDeployments().filter(ScriptDeployment::isActive)
        .map(s -> s.getStatusResult(streamEngine, Optional.empty())).collect(Collectors.toList());
  }

  //Option: drop table before create
  public PhysicalPlan compile(ScriptBundle bundle) throws Exception {
    ImportManager importManager = settings.getImportManagerProvider()
        .createImportManager(datasetRegistry);

    ErrorCollector errors = importManager.registerUserSchema(bundle.getMainScript().getSchema());

    if (errors.isFatal()) {
      throw new RuntimeException();
    }
    BundleOptions options = BundleOptions.builder()
        .importManager(importManager)
        .calciteEnv(new CalciteEnvironment())
        .schemaSettings(SchemaAdjustmentSettings.DEFAULT)
        .jdbcConfiguration(settings.getJdbcConfiguration())
        .streamEngine(settings.getStreamEngineProvider().create())
        .build();
    BundlePlanner bundlePlanner = new BundlePlanner(options);
    return bundlePlanner.processBundle(bundle);
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
