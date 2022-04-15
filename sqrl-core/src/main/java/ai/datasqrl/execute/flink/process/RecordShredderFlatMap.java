package ai.datasqrl.execute.flink.process;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class RecordShredderFlatMap implements FlatMapFunction<SourceRecord.Named, RowUpdate> {

    private final NamePath tableIdentifier;
    private final FieldProjection[] projections;

    public RecordShredderFlatMap(NamePath tableIdentifier, FieldProjection[] projections) {
        this.tableIdentifier = tableIdentifier;
        this.projections = projections;
    }

    @Override
    public void flatMap(SourceRecord.Named sourceRecord, Collector<RowUpdate> collector) throws Exception {
        Object[] cols = new Object[projections.length];
        int colno = 0;
        while (colno < projections.length && projections[colno].getDepth()==0) {
            cols[colno] = projections[colno].getData(sourceRecord);
            colno++;
        }
        //Construct rows iteratively
        constructRows(sourceRecord.getData(), cols, colno, 0, 0, collector, sourceRecord);
    }

    private void constructRows(Map<Name, Object> data, Object[] cols, int colno, int depth, int arrayIndex,
                               Collector<RowUpdate> collector, SourceRecord<Name> sourceRecord) {
        while (colno < projections.length && projections[colno].getDepth()==depth) {
            FieldProjection projection = projections[colno];
            //Deal with special case of array index
            if (projection instanceof FieldProjection.ArrayIndex) cols[colno] = arrayIndex;
            else cols[colno] = projection.getData(data);
            colno++;
        }
        //Need to go down one level or wrap up the record because its complete
        if (colno >= projections.length) {
            //We have projected out all fields, let's create Row and emit
            RowUpdate row = new RowUpdate.AppendOnly(sourceRecord.getIngestTime(), cols);
            //System.out.println("Shredded row: " + row);
            collector.collect(row);
        } else {
            assert tableIdentifier.getLength()>depth;
            //We have to get into nested table
            Object nested = data.get(tableIdentifier.get(depth));
            if (nested instanceof Map) {
                constructRows((Map) nested, cols, colno, depth + 1, 0, collector, sourceRecord);
            } else if (nested instanceof List) {
                List arr = (List)nested;
                for (int i = 0; i < arr.size(); i++) {
                    Object[] colcopy = cols.clone();
                    constructRows((Map) arr.get(i), colcopy, colno, depth + 1, i, collector, sourceRecord);
                }
            } else throw new IllegalArgumentException("Unexpected data encountered: " + nested);
        }
    }

}

