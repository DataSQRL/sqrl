package com.datasqrl;

import com.datasqrl.config.provider.JDBCConnectionProvider;
import com.datasqrl.io.impl.file.DirectoryDataSystem;
import com.datasqrl.io.impl.file.FilePath;
import com.datasqrl.io.sources.dataset.TableSink;
import com.datasqrl.physical.PhysicalPlan;
import com.datasqrl.physical.PhysicalPlanExecutor;
import com.datasqrl.physical.PhysicalPlanner;
import com.datasqrl.physical.database.QueryTemplate;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.RelToSql;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.local.analyze.ResolveTest;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.ResultSetPrinter;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestRelWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AbstractPhysicalSQRLIT extends AbstractLogicalSQRLIT {

    public JDBCConnectionProvider jdbc;
    public PhysicalPlanner physicalPlanner;

    protected SnapshotTest.Snapshot snapshot;

    protected void initialize(IntegrationTestSettings settings, Path rootDir) {
        super.initialize(settings, rootDir);

        jdbc = engineSettings.getJDBC();

        physicalPlanner = new PhysicalPlanner(planner.getRelBuilder());
    }


    protected void validateTables(String script, String... queryTables) {
        validateTables(script,Collections.EMPTY_SET,queryTables);
    }

    protected void validateTables(String script, Set<String> tableWithoutTimestamp, String... queryTables) {
        validateTables(script,Arrays.asList(queryTables), tableWithoutTimestamp, Set.of());
    }

    @SneakyThrows
    protected void validateTables(String script, Collection<String> queryTables, Set<String> tableWithoutTimestamp, Set<String> tableNoDataSnapshot) {
        ScriptNode node = parse(script);
        Resolve.Env resolvedDag = resolve.planDag(session, node);
        DAGPlanner dagPlanner = new DAGPlanner(planner, session.getPipeline());
        //We add a scan query for every query table
        List<APIQuery> queries = new ArrayList<APIQuery>();
        CalciteSchema relSchema = resolvedDag.getRelSchema();
        for (String tableName : queryTables) {
            Optional<VirtualRelationalTable> vtOpt = ResolveTest.getLatestTable(relSchema,tableName,VirtualRelationalTable.class);
            Preconditions.checkArgument(vtOpt.isPresent(),"No such table: %s",tableName);
            VirtualRelationalTable vt = vtOpt.get();
            RelNode rel = planner.getRelBuilder().scan(vt.getNameId()).build();
            queries.add(new APIQuery(tableName, rel));
        }
        OptimizedDAG dag = dagPlanner.plan(relSchema,queries, resolvedDag.getExports());
        addContent(dag);
        PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
        PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
        PhysicalPlanExecutor.Result result = executor.execute(physicalPlan);
        for (APIQuery query : queries) {
            QueryTemplate template = physicalPlan.getDatabaseQueries().get(query);
            String sqlQuery = RelToSql.convertToSql(template.getRelNode());
            System.out.println("Executing query: " + sqlQuery);
            ResultSet resultSet = jdbc.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
                    .executeQuery(sqlQuery);
            if (tableNoDataSnapshot.contains(query.getNameId())) {
                resultSet.last();
                int numResults = resultSet.getRow();
                System.out.println("Number of rows returned: " + numResults);
            } else {
                //Since Flink execution order is non-deterministic we need to sort results and remove uuid and ingest_time which change with every invocation
                Predicate<Integer> typeFilter = Predicates.alwaysTrue();
                if (tableWithoutTimestamp.contains(query.getNameId())) typeFilter = filterOutTimestampColumn;
                String content = Arrays.stream(ResultSetPrinter.toLines(resultSet,
                                s -> Stream.of("_uuid", "_ingest_time", "__").noneMatch(p -> s.startsWith(p)), typeFilter))
                        .sorted().collect(Collectors.joining(System.lineSeparator()));
                snapshot.addContent(content, query.getNameId(), "data");
            }
        }
        for (Resolve.ResolvedExport export : resolvedDag.getExports()) {
            TableSink sink = export.getSink();
            if (sink.getConnector() instanceof DirectoryDataSystem.Connector) {
                DirectoryDataSystem.Connector connector = (DirectoryDataSystem.Connector)sink.getConnector();
                FilePath path = connector.getPath().resolve(sink.getConfiguration().getIdentifier());
                Path filePath = Paths.get(path.toString());
                snapshot.addContent(String.valueOf(FileTestUtil.countLinesInAllPartFiles(filePath)),
                        "export",sink.getConfiguration().getIdentifier());
            }
        }
        snapshot.createOrValidate();
    }

    private void addContent(OptimizedDAG dag, String... caseNames) {
        dag.getWriteQueries().forEach(mq -> snapshot.addContent(TestRelWriter.explain(mq.getRelNode()),
            ArrayUtils.addAll(caseNames,mq.getSink().getName(),"lp-stream")));
        dag.getReadQueries().forEach(dq -> snapshot.addContent(TestRelWriter.explain(dq.getRelNode()),
            ArrayUtils.addAll(caseNames,dq.getQuery().getNameId(),"lp-database")));
    }

    protected static final Predicate<Integer> filterOutTimestampColumn =
            type -> type!= Types.TIMESTAMP_WITH_TIMEZONE && type!=Types.TIMESTAMP;


    protected ScriptNode parse(String query) {
        return parser.parse(query);
    }

}
