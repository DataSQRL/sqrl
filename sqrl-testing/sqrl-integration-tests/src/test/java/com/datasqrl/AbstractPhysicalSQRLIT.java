/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.StagePlan;
import com.datasqrl.engine.PhysicalPlanExecutor;
import com.datasqrl.engine.PhysicalPlanner;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.frontend.SqrlPhysicalPlan;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.io.impl.file.FileDataSystemConfig;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.impl.file.FilePath;
import com.datasqrl.io.impl.file.FilePathConfig;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.plan.local.analyze.FuzzingRetailSqrlModule;
import com.datasqrl.plan.local.analyze.MockAPIConnectorManager;
import com.datasqrl.plan.local.analyze.ResolveTest;
import com.datasqrl.plan.local.analyze.RetailSqrlModule;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.DatabaseHandle;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.ResultSetPrinter;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.util.TestRelWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.ClassRule;

@Slf4j
public class AbstractPhysicalSQRLIT extends AbstractLogicalSQRLIT {

  public SqrlPhysicalPlan physicalPlanner;
  JdbcDataSystemConnector jdbc;

  protected SnapshotTest.Snapshot snapshot;
  protected boolean closeSnapshotOnValidate = true;

  private static final int DEFAULT_PARALLELISM = 4;

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
              .build());

  @After
  public final void cleanupRunningJobs() throws Exception {
    if (!MINI_CLUSTER_RESOURCE.getMiniCluster().isRunning()) {
      // do nothing if the MiniCluster is not running
      log.warn("Mini cluster is not running after the test!");
      return;
    }

    for (JobStatusMessage path : MINI_CLUSTER_RESOURCE.getClusterClient().listJobs().get()) {
      if (!path.getJobState().isTerminalState()) {
        try {
          MINI_CLUSTER_RESOURCE.getClusterClient().cancel(path.getJobId()).get();
        } catch (Exception ignored) {
          // ignore exceptions when cancelling dangling jobs
        }
      }
    }
  }

  protected void initialize(IntegrationTestSettings settings, Path rootDir) {
    initialize(settings, rootDir, Optional.empty());
  }
  protected void initialize(IntegrationTestSettings settings, Path rootDir, Optional<Path> errorDir) {

    Map<NamePath, SqrlModule> addlModules = Map.of();
    CalciteTableFactory tableFactory = new CalciteTableFactory(framework);
    if (rootDir == null) {
      RetailSqrlModule retailSqrlModule = new RetailSqrlModule();
      retailSqrlModule.init(tableFactory);
      FuzzingRetailSqrlModule fuzzingRetailSqrlModule = new FuzzingRetailSqrlModule();
      fuzzingRetailSqrlModule.init(tableFactory);
      addlModules = Map.of(NamePath.of("ecommerce-data"), retailSqrlModule,
          NamePath.of("ecommerce-data-large"), fuzzingRetailSqrlModule);
    }
    Pair<DatabaseHandle, PipelineFactory> engines = settings.getSqrlSettings();
    this.database = engines.getLeft();

    SqrlTestDIModule module = new SqrlTestDIModule(engines.getRight().createPipeline(), settings, rootDir, addlModules, errorDir,
        ErrorCollector.root(), framework, NameCanonicalizer.SYSTEM);
    Injector injector = Guice.createInjector(module);

    super.initialize(settings, rootDir, injector);

    jdbc = injector.getInstance(ExecutionPipeline.class).getStages().stream()
      .filter(f->f.getEngine() instanceof JDBCEngine)
      .map(f->((JDBCEngine) f.getEngine()).getConnector())
      .findAny()
      .orElseThrow();

    physicalPlanner = injector.getInstance(SqrlPhysicalPlan.class);
  }


  protected void validateTables(String script, String... queryTables) {
    validateTables(script, Collections.EMPTY_SET, queryTables);
  }

  protected void validateTables(String script, Set<String> tableWithoutTimestamp,
      String... queryTables) {
    validateTables(script, Arrays.asList(queryTables), tableWithoutTimestamp, Set.of());
  }

  @SneakyThrows
  protected void validateTables(String script, Collection<String> queryTables,
      Set<String> tableWithoutTimestamp, Set<String> tableNoDataSnapshot) {
    Namespace ns = plan(script);

    //We add a scan query for every query table
    APIConnectorManager apiManager = new MockAPIConnectorManager();
    SqrlSchema sqrlSchema = planner.getSchema();
    for (String tableName : queryTables) {
      Optional<ScriptRelationalTable> vtOpt = ResolveTest.getLatestTable(sqrlSchema, tableName,
              ScriptRelationalTable.class);
      Preconditions.checkArgument(vtOpt.isPresent(), "No such table: %s", tableName);
      ScriptRelationalTable vt = vtOpt.get();
      RelBuilder relBuilder = planner.createRelBuilder().scan(vt.getNameId());
      relBuilder = CalciteUtil.projectOutNested(relBuilder);
      apiManager.addQuery(new APIQuery(tableName, relBuilder.build()));
    }

    PhysicalDAGPlan dag = physicalPlanner.planDag(planner.getFramework(), ns.getPipeline(),
        apiManager, null, true);
    addContent(dag);

    ErrorSink errorSink = injector.getInstance(ErrorSink.class);
    PhysicalPlan physicalPlan =  new PhysicalPlanner(framework, errorSink.getErrorSink())
        .plan(dag);
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
    CompletableFuture[] completableFutures = result.getResults().stream()
        .map(s->s.getResult())
        .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(completableFutures)
        .get();

    //todo: filter out jdbc engine
    StagePlan db = physicalPlan.getStagePlans().stream()
        .filter(e-> e.getStage().getEngine() instanceof JDBCEngine)
        .findAny()
        .orElseThrow();
    JDBCEngine dbEngine = (JDBCEngine) db.getStage().getEngine();
    Connection conn = DriverManager.getConnection(dbEngine.getConnector()
        .getUrl(), dbEngine.getConnector().getUser(),
        dbEngine.getConnector().getPassword());

    for (APIQuery query : apiManager.getQueries()) {
      QueryTemplate template = physicalPlan.getDatabaseQueries().get(query);

      String sqlQuery = framework.getQueryPlanner().relToString(Dialect.POSTGRES, template.getRelNode());
      log.info("Executing query for {}: {}", query.getNameId(), sqlQuery);

      ResultSet resultSet = conn
          .createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
          .executeQuery(sqlQuery);
      if (tableNoDataSnapshot.contains(query.getNameId())) {
        resultSet.last();
        int numResults = resultSet.getRow();
        log.info("Number of rows returned: " + numResults);
      } else {
        //Since Flink execution order is non-deterministic we need to sort results and remove uuid and ingest_time which change with every invocation
        Predicate<Integer> typeFilter = Predicates.alwaysTrue();
        if (tableWithoutTimestamp.contains(query.getNameId())) {
          typeFilter = filterOutTimestampColumn;
        }
        String content = Arrays.stream(ResultSetPrinter.toLines(resultSet,
                s -> !s.startsWith(ReservedName.UUID.getCanonical()) && !ReservedName.INGEST_TIME.matches(s) && !Name.isSystemHidden(s),
                typeFilter))
            .sorted().collect(Collectors.joining(System.lineSeparator()));
        snapshot.addContent(content, query.getNameId(), "data");
      }
    }
    StreamUtil.filterByClass(dag.getQueriesByType(PhysicalDAGPlan.WriteQuery.class).stream()
        .map(WriteQuery::getSink),ExternalSink.class)
        .map(ExternalSink::getTableSink)
        .filter(sink -> sink.getConfiguration().getConnectorName().equalsIgnoreCase(
            FileDataSystemFactory.SYSTEM_NAME))
        .forEach(sink -> {
          TableConfig tableConfig = sink.getConfiguration();
          FilePathConfig fpConfig = FileDataSystemConfig.fromConfig(tableConfig).getFilePath(tableConfig.getErrors());
          FilePath path = fpConfig.getDirectory()
              .resolve(tableConfig.getBase().getIdentifier());
          Path filePath = Paths.get(path.toString());
          snapshot.addContent(String.valueOf(FileTestUtil.countLinesInAllPartFiles(filePath)),
              "export", tableConfig.getBase().getIdentifier());
        });
    if (closeSnapshotOnValidate) snapshot.createOrValidate();
  }

  private void addContent(PhysicalDAGPlan dag, String... caseNames) {
    dag.getWriteQueries().stream().sorted(Comparator.comparing(wq -> wq.getSink().getName()))
        .forEach(mq -> snapshot.addContent(TestRelWriter.explain(mq.getRelNode()),
        ArrayUtils.addAll(caseNames, mq.getSink().getName(), "lp-stream")));
    dag.getReadQueries().stream().sorted(Comparator.comparing(rq -> rq.getQuery().getNameId()))
        .forEach(dq -> snapshot.addContent(TestRelWriter.explain(dq.getRelNode()),
        ArrayUtils.addAll(caseNames, dq.getQuery().getNameId(), "lp-database")));
  }

  protected static final Predicate<Integer> filterOutTimestampColumn =
      type -> type != Types.TIMESTAMP_WITH_TIMEZONE && type != Types.TIMESTAMP;

}
