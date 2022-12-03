package com.datasqrl.physical.stream.flink.schema;

import com.datasqrl.schema.converters.SourceRecord2RowMapper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Arrays;

public class FlinkRowConstructor implements SourceRecord2RowMapper.RowConstructor<Row> {

    public static final FlinkRowConstructor INSTANCE = new FlinkRowConstructor();

    @Override
    public Row createRow(Object[] columns) {
        return Row.ofKind(RowKind.INSERT, columns);
    }

    @Override
    public Row createNestedRow(Object[] columns) {
        return Row.of(columns);
    }

    @Override
    public Row[] createRowList(Object[] rows) {
        return Arrays.copyOf(rows,rows.length,Row[].class);
    }
}
