package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.stream.flink.FlinkStreamBuilder.SchemaToStreamContext;
import com.datasqrl.engine.stream.flink.schema.FlinkRowConstructor;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.io.tables.TableSchemaVisitor;
import com.datasqrl.schema.UniversalTableBuilder;
import com.datasqrl.schema.converters.SourceRecord2RowMapper;
import com.datasqrl.schema.input.FlexibleDatasetSchema.TableField;
import com.datasqrl.schema.input.FlexibleTable2UTBConverter;
import com.datasqrl.schema.input.FlexibleTableConverter;
import com.datasqrl.schema.input.InputTableSchema;
import com.datasqrl.schema.input.JsonTableSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class Schema2StreamVisitor implements TableSchemaVisitor<DataStream, SchemaToStreamContext> {

  @Override
  public DataStream accept(TableField tableField, SchemaToStreamContext context) {
    return context.getStream()
        .getStream().map(getRowMapper(context.getSchema()), getTypeInformation(context.getSchema()));
  }

  public static UniversalTableBuilder getUniversalTableBuilder(InputTableSchema schema) {
    FlexibleTableConverter converter = new FlexibleTableConverter(schema);
    FlexibleTable2UTBConverter utbConverter = new FlexibleTable2UTBConverter();
    return converter.apply(utbConverter);
  }

  public MapFunction<Named, Row> getRowMapper(InputTableSchema schema) {
    SourceRecord2RowMapper<Row> mapper = new SourceRecord2RowMapper(schema,
        FlinkRowConstructor.INSTANCE);
    return mapper::apply;
  }

  public TypeInformation getTypeInformation(InputTableSchema schema) {
    UniversalTableBuilder tblBuilder = getUniversalTableBuilder(schema);
    return new FlinkTypeInfoSchemaGenerator().convertSchema(
        tblBuilder);
  }

  @Override
  public DataStream accept(JsonTableSchema jsonTableSchema, SchemaToStreamContext context) {

    return null;
  }
}
