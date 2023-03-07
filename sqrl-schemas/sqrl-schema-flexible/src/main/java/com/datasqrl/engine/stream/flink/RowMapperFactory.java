/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.FlexibleSchemaRowMapper;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.input.FlexibleTable2UTBConverter;
import com.datasqrl.schema.input.FlexibleTableConverter;
import com.datasqrl.schema.input.InputTableSchema;
import java.util.Optional;
import org.apache.flink.api.common.functions.MapFunction;

public class RowMapperFactory {

  public static UniversalTable getFlexibleUniversalTableBuilder(TableSchema schema, boolean hasSourceTimestamp) {
    FlexibleTableConverter converter = new FlexibleTableConverter(schema, hasSourceTimestamp,
        Optional.empty());
    FlexibleTable2UTBConverter utbConverter = new FlexibleTable2UTBConverter(hasSourceTimestamp);
    return converter.apply(utbConverter);
  }

  public MapFunction<Named, Object> getRowMapper(InputTableSchema schema,
      RowConstructor rowConstructor) {
    FlexibleSchemaRowMapper mapper = new FlexibleSchemaRowMapper(schema.getSchema(), schema.isHasSourceTimestamp(),
        rowConstructor);
    return mapper::apply;
  }
}
