///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl;
//
//import com.datasqrl.calcite.Dialect;
//import com.datasqrl.canonicalizer.Name;
//import com.datasqrl.canonicalizer.ReservedName;
//import com.datasqrl.config.SqrlConfig;
//import com.datasqrl.engine.PhysicalPlan;
//import com.datasqrl.engine.PhysicalPlanExecutor;
//import com.datasqrl.engine.PhysicalPlanner;
//import com.datasqrl.engine.database.QueryTemplate;
//import com.datasqrl.graphql.APIConnectorManager;
//import com.datasqrl.graphql.inference.AbstractSchemaInferenceModelTest;
//import com.datasqrl.io.impl.file.FileDataSystemConfig;
//import com.datasqrl.io.impl.file.FileDataSystemFactory;
//import com.datasqrl.io.impl.file.FilePath;
//import com.datasqrl.io.impl.file.FilePathConfig;
//import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
//import com.datasqrl.io.tables.TableConfig;
//import com.datasqrl.plan.global.DAGPlanner;
//import com.datasqrl.plan.global.PhysicalDAGPlan;
//import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
//import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
//import com.datasqrl.plan.queries.APIQuery;
//import com.datasqrl.plan.table.ScriptRelationalTable;
//import com.datasqrl.util.CalciteUtil;
//import com.datasqrl.util.FileTestUtil;
//import com.datasqrl.util.ResultSetPrinter;
//import com.datasqrl.util.SnapshotTest;
//import com.datasqrl.util.StreamUtil;
//import com.datasqrl.plan.util.RelWriterWithHints;
//import com.google.common.base.Preconditions;
//import com.google.common.base.Predicates;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.Types;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.Optional;
//import java.util.Set;
//import java.util.concurrent.CompletableFuture;
//import java.util.function.Predicate;
//import java.util.stream.Collectors;
//import lombok.SneakyThrows;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.calcite.jdbc.SqrlSchema;
//import org.apache.calcite.tools.RelBuilder;
//import org.apache.commons.lang3.ArrayUtils;
//import org.apache.flink.test.junit5.MiniClusterExtension;
//import org.junit.jupiter.api.extension.ExtendWith;
//
//@Slf4j
//@ExtendWith(MiniClusterExtension.class)
//public class AbstractPhysicalSQRLIT extends AbstractSchemaInferenceModelTest {
//
//  protected SnapshotTest.Snapshot snapshot;
//  protected boolean closeSnapshotOnValidate = true;
//
//
//  protected void validateTables(String script, String... queryTables) {
//    validateTables(script, Collections.EMPTY_SET, queryTables);
//  }
//
//  protected void validateTables(String script, Set<String> tableWithoutTimestamp,
//      String... queryTables) {
//    validateTables(script, Arrays.asList(queryTables), tableWithoutTimestamp, Set.of());
//  }
// static int i =0;
//  @SneakyThrows
//  protected void validateTables(String script, Collection<String> queryTables,
//      Set<String> tableWithoutTimestamp, Set<String> tableNoDataSnapshot) {
//
////    injector.getInstance(SqrlConfig.class)
////        .toFile(Paths.get("/Users/henneberger/sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/usecase/c360/"+ snapshot.getFileName()+ ".package.json"));
////    Files.writeString(Paths.get("/Users/henneberger/sqrl/sqrl-testing/sqrl-integration-tests/src/test/resources/usecase/c360/"+ snapshot.getFileName()+ ".sqrl"),
////        script);
//    plan(script);
//
//    //We add a scan query for every query table
//    APIConnectorManager apiManager = injector.getInstance(APIConnectorManager.class);//new MockAPIConnectorManager(framework, pipeline, errors);
//    SqrlSchema sqrlSchema = framework.getSchema();
//    for (String tableName : queryTables) {
//      Optional<ScriptRelationalTable> vtOpt = getLatestTable(sqrlSchema, tableName,
//              ScriptRelationalTable.class);
//      Preconditions.checkArgument(vtOpt.isPresent(), "No such table: %s", tableName);
//      ScriptRelationalTable vt = vtOpt.get();
//      RelBuilder relBuilder = framework.getQueryPlanner().getRelBuilder()
//          .scan(vt.getNameId());
//      relBuilder = CalciteUtil.projectOutNested(relBuilder);
//      apiManager.addQuery(new APIQuery(tableName, null, relBuilder.build(), null, null, false));
//    }
//
//    DAGPlanner dagPlanner = injector.getInstance(DAGPlanner.class);
//    createAPIQueries(queryTables);
//    PhysicalDAGPlan dag = dagPlanner.plan();
//    addContent(dag);
//
//    PhysicalPlan physicalPlan = injector.getInstance(PhysicalPlanner.class)
//        .plan(dag);
//    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
//    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
//    CompletableFuture[] completableFutures = result.getResults().stream()
//        .map(s->s.getResult())
//        .toArray(CompletableFuture[]::new);
//    CompletableFuture.allOf(completableFutures)
//        .get();
//
//    JdbcDataSystemConnector jdbcDataSystemConnector = jdbc.get();
//    Connection conn = DriverManager.getConnection(jdbcDataSystemConnector.getUrl(),
//        jdbcDataSystemConnector.getUser(), jdbcDataSystemConnector.getPassword());
//
//    for (APIQuery query : apiManager.getQueries()) {
//      QueryTemplate template = physicalPlan.getDatabaseQueries().get(query);
//      String sqlQuery = framework.getQueryPlanner().relToString(Dialect.POSTGRES,
//              framework.getQueryPlanner().convertRelToDialect(Dialect.POSTGRES, template.getRelNode()))
//          .getSql();
//      log.info("Executing query for {}: {}", query.getNameId(), sqlQuery);
//
//      ResultSet resultSet = conn
//          .createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
//          .executeQuery(sqlQuery);
//      if (tableNoDataSnapshot.contains(query.getNameId())) {
//        resultSet.last();
//        int numResults = resultSet.getRow();
//        log.info("Number of rows returned: " + numResults);
//      } else {
//        //Since Flink execution order is non-deterministic we need to sort results and remove uuid and ingest_time which change with every invocation
//        Predicate<Integer> typeFilter = Predicates.alwaysTrue();
//        if (tableWithoutTimestamp.contains(query.getNameId())) {
//          typeFilter = filterOutTimestampColumn;
//        }
//        String content = Arrays.stream(ResultSetPrinter.toLines(resultSet,
//                s -> !s.startsWith(ReservedName.UUID.getCanonical()) && !ReservedName.INGEST_TIME.matches(s) && !Name.isSystemHidden(s),
//                typeFilter))
//            .sorted().collect(Collectors.joining(System.lineSeparator()));
//        snapshot.addContent(content, query.getNameId(), "data");
//      }
//    }
//    StreamUtil.filterByClass(dag.getQueriesByType(PhysicalDAGPlan.WriteQuery.class).stream()
//        .map(WriteQuery::getSink),ExternalSink.class)
//        .map(ExternalSink::getTableSink)
//        .filter(sink -> sink.getConfiguration().getConnectorName().equalsIgnoreCase(
//            FileDataSystemFactory.SYSTEM_NAME))
//        .forEach(sink -> {
//          TableConfig tableConfig = sink.getConfiguration();
//          FilePathConfig fpConfig = FileDataSystemConfig.fromConfig(tableConfig).getFilePath(tableConfig.getErrors());
//          FilePath path = fpConfig.getDirectory()
//              .resolve(tableConfig.getBase().getIdentifier());
//          Path filePath = Paths.get(path.toString());
//          snapshot.addContent(String.valueOf(FileTestUtil.countLinesInAllPartFiles(filePath)),
//              "export", tableConfig.getBase().getIdentifier());
//        });
//    if (closeSnapshotOnValidate) snapshot.createOrValidate();
//  }
//
//  private void addContent(PhysicalDAGPlan dag, String... caseNames) {
//    dag.getWriteQueries().stream().sorted(Comparator.comparing(wq -> wq.getSink().getName()))
//        .forEach(mq -> snapshot.addContent(RelWriterWithHints.explain(mq.getRelNode()),
//        ArrayUtils.addAll(caseNames, mq.getSink().getName(), "lp-stream")));
//    dag.getReadQueries().stream().sorted(Comparator.comparing(rq -> rq.getQuery().getNameId()))
//        .forEach(dq -> snapshot.addContent(RelWriterWithHints.explain(dq.getRelNode()),
//        ArrayUtils.addAll(caseNames, dq.getQuery().getNameId(), "lp-database")));
//  }
//
//  protected static final Predicate<Integer> filterOutTimestampColumn =
//      type -> type != Types.TIMESTAMP_WITH_TIMEZONE && type != Types.TIMESTAMP;
//
//}
