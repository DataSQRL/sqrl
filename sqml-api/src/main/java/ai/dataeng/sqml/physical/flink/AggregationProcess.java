package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.logical4.AggregateOperator;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.StreamType;
import ai.dataeng.sqml.physical.flink.aggregates.SimpleCount;
import lombok.Value;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class AggregationProcess extends KeyedProcessFunction<Row,RowUpdate,RowUpdate> {

    public static final String AGGREGATE_STATE_SUFFIX = "_aggregates";

    private final String tableId;
    private final AggregatorDefinition[] aggregatorDefinitions;
    private final StreamType streamType;

    private transient ValueState<List<Aggregator>> aggregatorStore;

    public AggregationProcess(String tableId, AggregatorDefinition[] aggregatorDefinitions, StreamType streamType) {
        this.tableId = tableId;
        this.aggregatorDefinitions = aggregatorDefinitions;
        this.streamType = streamType;
    }

    @Override
    public void processElement(RowUpdate rowUpdate, KeyedProcessFunction<Row, RowUpdate, RowUpdate>.Context context,
                               Collector<RowUpdate> collector) throws Exception {
        boolean initialized = false;
        List<Aggregator> aggregators = aggregatorStore.value();
        if (aggregators==null) { //Initialize
            aggregators = new ArrayList<>(aggregatorDefinitions.length);
            for (int i = 0; i < aggregatorDefinitions.length; i++) {
                aggregators.add(aggregatorDefinitions[i].aggregatorFactory.get());
            }
            initialized = true;
        }

        Aggregator.Update[] updates = new Aggregator.Update[aggregatorDefinitions.length];
        boolean aggregatorUpdated = initialized;
        boolean aggregateChanged = false;
        for (int i = 0; i < aggregatorDefinitions.length; i++) {
            Object[] append = assembleArguments(rowUpdate.getAppend(), aggregatorDefinitions[i]);
            Object[] retract = assembleArguments(rowUpdate.getRetraction(), aggregatorDefinitions[i]);
            if (!Arrays.deepEquals(append,retract)) {
                updates[i]=aggregators.get(i).add(append,retract);
                aggregatorUpdated=true;
                aggregateChanged |= !Objects.equals(updates[i].newValue,updates[i].oldValue);
            } else {
                Object value = aggregators.get(i).getValue();
                updates[i]=new Aggregator.Update(value,value);
            }
        }
        if (aggregatorUpdated) aggregatorStore.update(aggregators);

        if (aggregateChanged) {
            //Construct output RowUpdate
            Row append = assembleOutputRow(context.getCurrentKey(),updates,true);
            if (streamType==StreamType.APPEND || initialized) {
                collector.collect(new RowUpdate.AppendOnly(rowUpdate,append));
            } else {
                Row retract = assembleOutputRow(context.getCurrentKey(),updates,false);
                collector.collect(new RowUpdate.Full(rowUpdate,append,retract));
            }
        }
    }

    private Row assembleOutputRow(Row keys, Aggregator.Update[] updates, boolean append /*false=retract*/) {
        Object[] cols = new Object[keys.getArity()+updates.length];
        System.arraycopy(keys.getRawValues(),0,cols,0,keys.getArity());
        int offset=keys.getArity();
        for (int i = 0; i < updates.length; i++) {
            Aggregator.Update update = updates[i];
            cols[offset++]=append?update.newValue:update.oldValue;
        }
        return new Row(cols);
    }

    private Object[] assembleArguments(Row row, AggregatorDefinition aggDef) {
        if (row==null) return null;
        int[] columnIndexes = aggDef.columnIndexes;
        Object[] colVals = new Object[columnIndexes.length];
        for (int col = 0; col < columnIndexes.length; col++) {
            colVals[col]=row.getValue(columnIndexes[col]);
        }
        return colVals;
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<List<Aggregator>> aggregatesDesc =
                new ValueStateDescriptor<>(tableId + AGGREGATE_STATE_SUFFIX,
                        TypeInformation.of(new TypeHint<List<Aggregator>>() {
                        }));
        aggregatorStore = getRuntimeContext().getState(aggregatesDesc);
    }

    public static AggregationProcess from(AggregateOperator aggOp) {
        String tableId = aggOp.getOutputSchema()[0][0].getTable().getId();
        AggregatorDefinition[] defs = new AggregatorDefinition[aggOp.getAggregates().size()];
        int i = 0;
        for (AggregateOperator.Aggregation agg : aggOp.getAggregates().values()) {
            defs[i] = convert(agg, aggOp);
        }
        return new AggregationProcess(tableId,defs,StreamType.RETRACT);
    }

    private static AggregatorDefinition convert(AggregateOperator.Aggregation agg,
                                                AggregateOperator aggOp) {
        LogicalPlan.Column[][] schema = aggOp.getInput().getOutputSchema();
        int[] columnIndexes = new int[agg.getArguments().size()];
        for (int i = 0; i < columnIndexes.length; i++) {
            columnIndexes[i]=agg.getArguments().get(i).getRowOffset(schema);
        }
        Supplier<? extends Aggregator> aggFactory;
        switch (agg.getFunctionHandle()) {
            case COUNT:
                aggFactory = new SimpleCount.Factory();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported aggregate function: " + agg.getFunctionHandle());
        }
        return new AggregatorDefinition(columnIndexes, aggFactory);
    }

    public static class AggregatorDefinition implements Serializable {

        private int[] columnIndexes;
        private Supplier<? extends Aggregator> aggregatorFactory;

        private AggregatorDefinition() {} //Kryo

        public AggregatorDefinition(int[] columnIndexes, Supplier<? extends Aggregator> aggregatorFactory) {
            this.columnIndexes = columnIndexes;
            this.aggregatorFactory = aggregatorFactory;
        }
    }

    public interface Aggregator extends Serializable {

        Update add(@Nullable Object[] append, @Nullable Object[] retraction);

        Object getValue();

        //void merge(Aggregator other);

        @Value
        class Update {
            Object newValue;
            Object oldValue;
        }

    }

}
