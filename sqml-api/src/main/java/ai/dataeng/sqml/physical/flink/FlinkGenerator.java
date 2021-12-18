package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.ingest.schema.SchemaValidationProcess;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.logical4.*;
import ai.dataeng.sqml.optimizer.LogicalPlanOptimizer;
import ai.dataeng.sqml.optimizer.MaterializeSink;
import ai.dataeng.sqml.optimizer.MaterializeSource;
import ai.dataeng.sqml.physical.DatabaseSink;
import ai.dataeng.sqml.tree.name.Name;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class FlinkGenerator {

    public static final String SCHEMA_ERROR_OUTPUT = "schema-error";

    private final FlinkConfiguration configuration;
    private final EnvironmentFactory envProvider;
    final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>(SCHEMA_ERROR_OUTPUT){}; //TODO: can we use one for all or do they need to be unique?

    public FlinkGenerator(FlinkConfiguration configuration, EnvironmentFactory envProvider) {
        this.configuration = configuration;
        this.envProvider = envProvider;
    }

    public Map<Name, Table> generateShreddedTables(DocumentSource source) {
        StreamExecutionEnvironment flinkEnv = envProvider.create();
        TableEnvironment tableEnv = envProvider.createTableApi();

        DataStream<SourceRecord<String>> stream = source.getTable().getDataStream(flinkEnv);
        SingleOutputStreamOperator<SourceRecord<Name>> validate = stream.process(
            new SchemaValidationProcess(schemaErrorTag, source.getSourceSchema(),
            source.getSettings(), source.getTable().getDataset().getRegistration()));
//

//
//        FileSource fileSource = new FileSource.FileSourceBuilder()
//            .build();
//        String ddl =
//            "CREATE TABLE orders (\n"
//                + "  user_id INT,\n"
//                + "  product STRING,\n"
//                + "  amount INT,\n"
//                + "  ts TIMESTAMP(3),\n"
//                + "  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n"
//                + ") WITH (\n"
//                + "  'connector.type' = 'filesystem',\n"
//                + "  'connector.path' = '"
//                + path
//                + "',\n"
//                + "  'format.type' = 'json'\n"
//                + ")";

//        tenv.sqlUpdate(
//            s"""
//         |CREATE TABLE kafkaTable (
//         | user_id VARCHAR,
//         | item_id VARCHAR,
//         | category_id VARCHAR,
//         | behavior STRING,
//         | ts TIMESTAMP
//         |) WITH (
//         | 'connector.type' = 'kafka', -- 使用kafka的连接器
//         | 'connector.version' = 'universal', -- universal代表使用0.11以后，如果0.9则直接写0.9
//         | 'connector.topic' = 'test',
//         | 'connector.properties.0.key' = 'zookeeper.connect',
//         | 'connector.properties.0.value' = 'hadoop01:2181,hadoop02:2181,hadoop03:2181',
//         | 'connector.properties.1.key' = 'bootstrap.servers',
//         | 'connector.properties.1.value' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
//         | 'update-mode' = 'append', -- 数据更新以追加形式
//         | 'connector.startup-mode' = 'latest-offset', //数据为主题中最新数据
//         | 'format.type' = 'json', -- 数据格式为json
//         | 'format.derive-schema' = 'true'  -- 从DDL schemal确定json解析规则
//         |)
//         |""".stripMargin)


//
//        Table apm = btEnv.fromValues(
//            row("{'appid':1, 'content':{'a':1}}"),
//            row("[{'appid':1, 'content':{'a':2}},{'appid':3, 'content':{'a':3}}]")
//        ).as("line");
//
//
//        // 注册函数
//        btEnv.createTemporaryFunction("jsonToMap", new JsonToMapTableFunc());
//
//        //
//        Table table = btEnv.sqlQuery("" +
//            " SELECT " +
//            "    data['appid'] AS appid, " +
//            "    data['content'] AS content " +
//            " FROM " + apm + " LEFT JOIN LATERAL TABLE(jsonToMap(line)) AS t(data) ON TRUE");
//

//        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", null);

        tableEnv.useCatalog("myhive");
        tableEnv.useDatabase("flink_db");


        return null;
//        shredder.getInput().
//        getInput(lp2pp, shredder.getInput()).flatMap(new RecordShredderFlatMap(shredder.getTableIdentifier(), shredder.getProjections()));

    }

    public StreamExecutionEnvironment generateStream(LogicalPlanOptimizer.Result logical, Map<MaterializeSource, DatabaseSink> sinkMapper) {
        StreamExecutionEnvironment flinkEnv = envProvider.create();

        final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>(SCHEMA_ERROR_OUTPUT){}; //TODO: can we use one for all or do they need to be unique?

        //Maps logical plan (lp) elements to physical plan (pp) elements
        Map<LogicalPlan.Node, DataStream> lp2pp = new HashMap<>();
        LogicalPlanIterator lpiter = new LogicalPlanIterator(logical.getStreamLogicalPlan());
        //The iterator guarantees that we will only visit nodes once we have visited all inputs, hence we can
        //construct the physical DataStream bottoms up and lookup previously constructed elements in lp2pp
        while (lpiter.hasNext()) {
            LogicalPlan.Node node = lpiter.next();
            DataStream converted = null;
            if (node instanceof DocumentSource) {
                DocumentSource source = (DocumentSource) node;
                DataStream<SourceRecord<String>> stream = source.getTable().getDataStream(flinkEnv);
                SingleOutputStreamOperator<SourceRecord<Name>> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, source.getSourceSchema(),
                        source.getSettings(), source.getTable().getDataset().getRegistration()));
                //validate.getSideOutput(schemaErrorTag).addSink(new PrintSinkFunction<>()); //TODO: handle errors
                converted = validate;
            } else if (node instanceof ShreddingOperator) {
                ShreddingOperator shredder = (ShreddingOperator) node;
                converted = getInput(lp2pp, shredder.getInput()).flatMap(new RecordShredderFlatMap(shredder.getTableIdentifier(), shredder.getProjections()));
            } else if (node instanceof FilterOperator) {
                FilterOperator filter = (FilterOperator) node;
                converted = getInput(lp2pp, filter.getInput()).flatMap(new FilterProcess(filter.getPredicate()));
            } else if (node instanceof AggregateOperator) {
                AggregateOperator agg = (AggregateOperator) node;
                DataStream<RowUpdate> input = getInput(lp2pp, agg.getInput());
                //First, we key by the group-by keys
                RowKeySelector keySelector = RowKeySelector.from(agg.getGroupByKeys().values(),agg.getInput());
                if (agg.getInput().getStreamType()==StreamType.RETRACT) {
                    //If we are dealing with a retract stream, we have to separate RowUpdate's if they differ on keys
                    input = input.flatMap(keySelector.getRowSeparator());
                }
                converted = input.keyBy(keySelector).process(AggregationProcess.from(agg));
            } else if (node instanceof MaterializeSink) {
                MaterializeSink sink = (MaterializeSink) node;
                DataStream input = getInput(lp2pp, sink.getInput());
                DatabaseSink dbsink = sinkMapper.get(sink.getSource());
                DatabaseUtil dbUtil = new DatabaseUtil(configuration);
                Preconditions.checkNotNull(dbsink);
                //bifurcate stream if we need to deal with deletes
                if (sink.getInput().getStreamType()==StreamType.RETRACT) {
                    input.filter(new DatabaseUtil.RowUpdateFilter(RowUpdate.Type.UPDATE, RowUpdate.Type.INSERT))
                            .addSink(dbUtil.getDatabaseSink(dbsink.getUpsertQuery(), StreamType.APPEND));
                    input.filter(new DatabaseUtil.RowUpdateFilter(RowUpdate.Type.DELETE))
                            .addSink(dbUtil.getDatabaseSink(dbsink.getDeleteQuery(), StreamType.RETRACT));
                } else {
                    input.addSink(dbUtil.getDatabaseSink(dbsink.getUpsertQuery(), StreamType.APPEND));
                }
                input.addSink(new PrintSinkFunction<>()); //TODO: remove, debug only
            } else if(node instanceof ProjectOperator) {
                converted = getInput(lp2pp, ((ProjectOperator)node).getInput());
            } else {
                throw new UnsupportedOperationException(node.getClass().getName());
            }

            if (converted != null) {
                lp2pp.put(node, converted);
            }
        }

        return flinkEnv;
    }

    private static<S extends DataStream> S getInput(Map<LogicalPlan.Node, DataStream> lp2pp, LogicalPlan.Node node) {
        return (S)lp2pp.get(node);
    }



}
