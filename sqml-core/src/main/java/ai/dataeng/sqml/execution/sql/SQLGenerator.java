//package ai.dataeng.sqml.execution.sql;
//
//import ai.dataeng.sqml.config.provider.JDBCConnectionProvider;
//import ai.dataeng.sqml.planner.Column;
//import ai.dataeng.sqml.planner.operator.AccessNode;
//import ai.dataeng.sqml.planner.LogicalPlanImpl;
//import ai.dataeng.sqml.planner.LogicalPlanIterator;
//import ai.dataeng.sqml.planner.optimize.LogicalPlanResult;
//import ai.dataeng.sqml.planner.optimize.MaterializeSource;
//import ai.dataeng.sqml.execution.sql.util.CreateTableBuilder;
//import ai.dataeng.sqml.execution.sql.util.DatabaseUtil;
//import ai.dataeng.sqml.execution.sql.util.TableBuilder;
//import lombok.AllArgsConstructor;
//import lombok.Value;
//
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//@AllArgsConstructor
//public class SQLGenerator {
//
//    JDBCConnectionProvider configuration;
//    //TODO: generate CREATE TABLE and CREATE VIEW statements for QueryNodes and relationships
//
//    public Result generateDatabase(LogicalPlanResult logical) {
//        Map<LogicalPlanImpl.Node, TableBuilder> lp2pp = new HashMap<>();
//        Map<MaterializeSource, DatabaseSink> sinkMapper = new HashMap<>();
//        Map<AccessNode, String> toTable = new HashMap<>();
//        List<String> dmlQueries = new ArrayList<>();
//        DatabaseUtil dbUtil = new DatabaseUtil(configuration);
//        LogicalPlanIterator lpiter = new LogicalPlanIterator(logical.getReadLogicalPlan());
//        //The iterator guarantees that we will only visit nodes once we have visited all inputs, hence we can
//        //construct the physical DataStream bottoms up and lookup previously constructed elements in lp2pp
//        while (lpiter.hasNext()) {
//            LogicalPlanImpl.Node node = lpiter.next();
//            TableBuilder tableBuilder = null;
//            if (node instanceof MaterializeSource) {
//                //Create table and build database sinks
//                MaterializeSource source = (MaterializeSource) node;
//                String tableName = source.getTable().getId();
//                Column[] tableSchema = source.getTableSchema();
//                tableBuilder = new CreateTableBuilder(tableName,dmlQueries,dbUtil).addColumns(tableSchema);
//                sinkMapper.put(source, dbUtil.getSink(tableName, tableSchema));
//            } else if (node instanceof AccessNode) {
//                //Do we need to convert relationships for the GraphQL API?
//                AccessNode access = (AccessNode) node;
//                TableBuilder tb = lp2pp.get(access.getInput());
//                toTable.put(access,tb.finish());
//            } else throw new UnsupportedOperationException("Not yet implemented: " + node);
//            if (tableBuilder!=null) {
//                lp2pp.put(node,tableBuilder);
//            }
//        }
//        return new Result(configuration, dmlQueries, sinkMapper, toTable);
//    }
//
//    @Value
//    public static class Result {
//
//        JDBCConnectionProvider configuration;
//        List<String> dmlQueries;
//
//        Map<MaterializeSource, DatabaseSink> sinkMapper;
//        Map<AccessNode, String> toTable;
//        //Map relationships for GraphQL API?
//
//        public void executeDMLs() {
//            String dmls = dmlQueries.stream().collect(Collectors.joining("\n"));
//            System.out.println(dmls);
//            try (Connection conn = configuration.getConnection(); Statement stmt = conn.createStatement()) {
//                stmt.executeUpdate(dmls);
//            } catch (SQLException e) {
//                throw new RuntimeException("Could not execute SQL query",e);
//            } catch (ClassNotFoundException e) {
//                throw new RuntimeException("Could not load database driver",e);
//            }
//        }
//    }
//
//}
