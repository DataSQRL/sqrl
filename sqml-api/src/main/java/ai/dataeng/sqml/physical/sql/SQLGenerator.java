package ai.dataeng.sqml.physical.sql;

import ai.dataeng.sqml.logical4.AccessNode;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlanIterator;
import ai.dataeng.sqml.optimizer.LogicalPlanOptimizer;
import ai.dataeng.sqml.optimizer.MaterializeSource;
import ai.dataeng.sqml.physical.DatabaseSink;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.jooq.DSLContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class SQLGenerator {

    SQLConfiguration configuration;
    //TODO: generate CREATE TABLE and CREATE VIEW statements for QueryNodes and relationships

    public Result generateDatabase(LogicalPlanOptimizer.Result logical) {
        Map<LogicalPlan.Node, TableBuilder> lp2pp = new HashMap<>();
        Map<MaterializeSource, DatabaseSink> sinkMapper = new HashMap<>();
        Map<AccessNode, String> toTable = new HashMap<>();
        List<String> dmlQueries = new ArrayList<>();
        DatabaseUtil dbUtil = new DatabaseUtil(configuration);
        LogicalPlanIterator lpiter = new LogicalPlanIterator(logical.getReadLogicalPlan());
        //The iterator guarantees that we will only visit nodes once we have visited all inputs, hence we can
        //construct the physical DataStream bottoms up and lookup previously constructed elements in lp2pp
        while (lpiter.hasNext()) {
            LogicalPlan.Node node = lpiter.next();
            TableBuilder tableBuilder = null;
            if (node instanceof MaterializeSource) {
                //Create table and build database sinks
                MaterializeSource source = (MaterializeSource) node;
                String tableName = source.getTable().getId();
                LogicalPlan.Column[] tableSchema = source.getTableSchema();
                tableBuilder = new CreateTableBuilder(tableName,tableSchema,dmlQueries,dbUtil);
                sinkMapper.put(source, dbUtil.getSink(tableName, tableSchema));
            } else if (node instanceof AccessNode) {
                //Do we need to convert relationships for the GraphQL API?
                AccessNode access = (AccessNode) node;
                TableBuilder tb = lp2pp.get(access.getInput());
                toTable.put(access,tb.finish());
            } else throw new UnsupportedOperationException("Not yet implemented: " + node);
            if (tableBuilder!=null) {
                lp2pp.put(node,tableBuilder);
            }
        }
        return new Result(configuration, dmlQueries, sinkMapper, toTable);
    }

    private abstract class TableBuilder {

        final String tableName;
        boolean isFinished;

        final List<String> dmlQueries;
        final DatabaseUtil dbUtil;

        private TableBuilder(String tableName, List<String> dmlQueries, DatabaseUtil dbUtil) {
            this.tableName = tableName;
            this.dmlQueries = dmlQueries;
            this.dbUtil = dbUtil;
        }

        public abstract String getSQL();

        public String finish() {
            if (!isFinished) {
                dmlQueries.add(getSQL());
                isFinished = true;
            }
            return tableName;
        }

    }

    private class CreateTableBuilder extends TableBuilder {

        private final String sql;

        private CreateTableBuilder(String tableName, LogicalPlan.Column[] tableSchema,
                                   List<String> dmlQueries, DatabaseUtil dbUtil) {
            super(tableName, dmlQueries, dbUtil);
            sql = dbUtil.createTableDML(tableName, tableSchema);
        }

        @Override
        public String getSQL() {
            return sql;
        }
    }

    private class ViewBuilder extends TableBuilder {

        private ViewBuilder(String tableName, List<String> dmlQueries, DatabaseUtil dbUtil) {
            super(tableName, dmlQueries, dbUtil);
        }

        @Override
        public String getSQL() {
            return null; //TODO
        }
    }

    @Value
    public static class Result {

        SQLConfiguration configuration;
        List<String> dmlQueries;

        Map<MaterializeSource, DatabaseSink> sinkMapper;
        Map<AccessNode, String> toTable;
        //Map relationships for GraphQL API?

        public void executeDMLs() {
            try {
                DSLContext context = configuration.getJooQ();
                for (String dml : dmlQueries) {
                    context.execute(dml);
                }
            } catch (SQLException e) {
                throw new RuntimeException("Could not execute SQL query",e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not load database driver",e);
            } catch (Exception e) {
                throw new RuntimeException("Encountered exception",e);
            }
        }
    }

}
