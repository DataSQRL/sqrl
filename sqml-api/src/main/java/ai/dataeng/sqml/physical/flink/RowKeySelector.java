package ai.dataeng.sqml.physical.flink;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.util.Collection;

public class RowKeySelector implements KeySelector<RowUpdate, Row> {

    private int[] indexes;

    public RowKeySelector() {} //Kryo

    public RowKeySelector(int[] indexes) {
        this.indexes = indexes;
    }

    @Override
    public Row getKey(RowUpdate update) throws Exception {
        Row key = getKey(update.hasAddition()?update.getAddition():update.getRetraction());
        assert update.getType()!=RowUpdate.Type.UPDATE || key.equals(getKey(update.getRetraction())); //Those should have been separated by KeyedRowSeperator
        return key;
    }

    private Row getKey(Row data) {
        Object[] keyValues = new Object[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            keyValues[i]=data.getValue(indexes[i]);
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
        return new KeyedRowSeparator(indexes);
    }

    public static class KeyedRowSeparator implements FlatMapFunction<RowUpdate,RowUpdate> {

        private int[] indexes;

        public KeyedRowSeparator() {} //Kryo

        public KeyedRowSeparator(int[] indexes) {
            this.indexes = indexes;
        }

        @Override
        public void flatMap(RowUpdate rowUpdate, Collector<RowUpdate> collector) throws Exception {

        }
    }
}
