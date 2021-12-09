package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.util.Collection;

public class RowKeySelector implements KeySelector<RowUpdate, Row> {

    private int[] keyIndexes;

    public RowKeySelector() {} //Kryo

    public RowKeySelector(int[] keyIndexes) {
        this.keyIndexes = keyIndexes;
    }

    @Override
    public Row getKey(RowUpdate update) throws Exception {
        Row key = getKey(update.hasAppend()?update.getAppend():update.getRetraction());
        assert update.getType()!=RowUpdate.Type.UPDATE || key.equals(getKey(update.getRetraction())); //Those should have been separated by KeyedRowSeperator
        return key;
    }

    private Row getKey(Row data) {
        return getKey(data,keyIndexes);
    }

    private static Row getKey(Row data, int[] keyIndexes) {
        Object[] keyValues = new Object[keyIndexes.length];
        for (int i = 0; i < keyIndexes.length; i++) {
            keyValues[i]=data.getValue(keyIndexes[i]);
        }
        return new Row(keyValues);
    }

    public static RowKeySelector from(Collection<ColumnReferenceExpression> keys, LogicalPlan.RowNode input) {
        LogicalPlan.Column[][] inputSchema = input.getOutputSchema();
        int[] indexes = new int[keys.size()];
        int offset = 0;
        for (ColumnReferenceExpression key : keys) {
            indexes[offset++] = key.getRowOffset(inputSchema);
        }
        return new RowKeySelector(indexes);
    }

    public KeyedRowSeparator getRowSeparator() {
        return new KeyedRowSeparator(keyIndexes);
    }

    public static class KeyedRowSeparator implements FlatMapFunction<RowUpdate,RowUpdate> {

        private int[] keyIndexes;

        public KeyedRowSeparator() {} //Kryo

        public KeyedRowSeparator(int[] keyIndexes) {
            this.keyIndexes = keyIndexes;
        }

        @Override
        public void flatMap(RowUpdate rowUpdate, Collector<RowUpdate> collector) throws Exception {
            boolean separateUpdate = false;
            if (rowUpdate.getType() == RowUpdate.Type.UPDATE) {
                //if the key columns have different values we need to separate them
                separateUpdate = !getKey(rowUpdate.getAppend(),keyIndexes).equals(getKey(rowUpdate.getRetraction(),keyIndexes));
            }
            if (separateUpdate) {
                collector.collect(new RowUpdate.Full(rowUpdate, rowUpdate.getAppend(), null));
                collector.collect(new RowUpdate.Full(rowUpdate, null, rowUpdate.getRetraction()));
            } else {
                collector.collect(rowUpdate);
            }
        }
    }
}
