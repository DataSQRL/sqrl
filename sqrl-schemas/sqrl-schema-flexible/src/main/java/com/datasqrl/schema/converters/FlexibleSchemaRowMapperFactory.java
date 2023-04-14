/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.input.FlexibleTable2UTBConverter;
import com.datasqrl.schema.input.FlexibleTableConverter;
import java.util.Optional;

public class FlexibleSchemaRowMapperFactory {

  public static UniversalTable getFlexibleUniversalTableBuilder(TableSchema schema, boolean hasSourceTimestamp,
      Optional<Name> tblAlias) {
    FlexibleTableConverter converter = new FlexibleTableConverter(schema, hasSourceTimestamp,
        tblAlias);
    FlexibleTable2UTBConverter utbConverter = new FlexibleTable2UTBConverter(hasSourceTimestamp);
    return converter.apply(utbConverter);
  }
}
