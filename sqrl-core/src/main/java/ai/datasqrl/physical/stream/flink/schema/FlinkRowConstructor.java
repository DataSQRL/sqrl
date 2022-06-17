package ai.datasqrl.physical.stream.flink.schema;

import ai.datasqrl.schema.converters.SourceRecord2RowMapper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class FlinkRowConstructor implements SourceRecord2RowMapper.RowConstructor<Row, Row> {

    public static final FlinkRowConstructor INSTANCE = new FlinkRowConstructor();

    @Override
    public Row createRoot(Object[] columns) {
        return Row.ofKind(RowKind.INSERT, columns);
    }

    @Override
    public Row createNested(Object[] columns) {
        return Row.of(columns);
    }

    @Override
    public Row[] createRowArray(int size) {
        return new Row[size];
    }
}
