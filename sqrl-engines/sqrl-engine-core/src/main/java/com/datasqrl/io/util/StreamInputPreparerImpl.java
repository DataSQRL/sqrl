/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.util;

import com.datasqrl.engine.stream.MapText2Raw;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.StreamSourceProvider;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;

public class StreamInputPreparerImpl implements StreamInputPreparer {

  public static final String PARSE_ERROR_TAG = "parse";

  private final SchemaAdjustmentSettings schemaAdjustmentSettings = SchemaAdjustmentSettings.DEFAULT;

  public boolean isRawInput(TableInput table) {
    //TODO: support other flexible formats
    return table.getParser() instanceof TextLineFormat.Parser;
  }

  @Override
  public StreamHolder<SourceRecord.Raw> getRawInput(TableInput table,
      StreamSourceProvider builder, ErrorLocation errorLocation) {
    Preconditions.checkArgument(isRawInput(table), "Not a valid raw input table: " + table);
    Format.Parser parser = table.getParser();
    if (parser instanceof TextLineFormat.Parser) {
      return text2Record(builder.fromTextSource(table),
          (TextLineFormat.Parser) parser, errorLocation);
    } else {
      throw new UnsupportedOperationException("Should never happen");
    }
  }

  public StreamHolder<SourceRecord.Raw> text2Record(
      StreamHolder<TimeAnnotatedRecord<String>> textSource,
      TextLineFormat.Parser textparser, ErrorLocation errorLocation) {
    return textSource.mapWithError(new MapText2Raw(textparser), errorLocation, SourceRecord.Raw.class);
  }

}
