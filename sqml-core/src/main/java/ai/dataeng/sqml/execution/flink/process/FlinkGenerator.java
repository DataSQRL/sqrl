package ai.dataeng.sqml.execution.flink.process;

import ai.dataeng.sqml.execution.flink.environment.EnvironmentFactory;
import ai.dataeng.sqml.execution.flink.ingest.schema.SchemaValidationProcess;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceRecord;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.LogicalPlanIterator;
import ai.dataeng.sqml.planner.operator.AggregateOperator;
import ai.dataeng.sqml.planner.operator.DocumentSource;
import ai.dataeng.sqml.planner.operator.FilterOperator;
import ai.dataeng.sqml.planner.operator.ProjectOperator;
import ai.dataeng.sqml.planner.operator.ShreddingOperator;
import ai.dataeng.sqml.planner.operator.StreamType;
import ai.dataeng.sqml.planner.optimize.LogicalPlanOptimizer;
import ai.dataeng.sqml.planner.optimize.MaterializeSink;
import ai.dataeng.sqml.planner.optimize.MaterializeSource;
import ai.dataeng.sqml.execution.sql.DatabaseSink;
import ai.dataeng.sqml.tree.name.Name;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
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

    public StreamExecutionEnvironment generateStream(LogicalPlanOptimizer.Result logical, Map<MaterializeSource, DatabaseSink> sinkMapper) {
        StreamExecutionEnvironment flinkEnv = envProvider.create();

        final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>(SCHEMA_ERROR_OUTPUT){}; //TODO: can we use one for all or do they need to be unique?

        //Maps logical plan (lp) elements to physical plan (pp) elements
        Map<LogicalPlanImpl.Node, DataStream> lp2pp = new HashMap<>();
        LogicalPlanIterator lpiter = new LogicalPlanIterator(logical.getStreamLogicalPlan());
        //The iterator guarantees that we will only visit nodes once we have visited all inputs, hence we can
        //construct the physical DataStream bottoms up and lookup previously constructed elements in lp2pp
        while (lpiter.hasNext()) {
            LogicalPlanImpl.Node node = lpiter.next();
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

    private static<S extends DataStream> S getInput(Map<LogicalPlanImpl.Node, DataStream> lp2pp, LogicalPlanImpl.Node node) {
        return (S)lp2pp.get(node);
    }



}
