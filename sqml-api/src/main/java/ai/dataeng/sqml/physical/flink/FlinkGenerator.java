package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.flink.EnvironmentFactory;
import ai.dataeng.sqml.ingest.schema.SchemaValidationProcess;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.logical4.*;
import ai.dataeng.sqml.optimizer.LogicalPlanOptimizer;
import ai.dataeng.sqml.optimizer.MaterializeSink;
import ai.dataeng.sqml.tree.name.Name;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class FlinkGenerator {

    public static final String SCHEMA_ERROR_OUTPUT = "schema-error";

    private final EnvironmentFactory envProvider;

    public FlinkGenerator(EnvironmentFactory envProvider) {
        this.envProvider = envProvider;
    }

    public StreamExecutionEnvironment generateStream(LogicalPlanOptimizer.Result logical) {
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
                converted = getInput(lp2pp, filter.getInput()).flatMap(new FilterFunction(filter.getPredicate()));
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
                getInput(lp2pp, sink.getInput()).addSink(new PrintSinkFunction<>()); //TODO: .addSink(dbSinkFactory.getSink(shreddedTableName,shredder.getResultSchema()));
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
