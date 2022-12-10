/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.util;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

public class StreamInputPreparerImpl implements StreamInputPreparer {

  public static final String PARSE_ERROR_TAG = "parse";

  private final SchemaAdjustmentSettings schemaAdjustmentSettings = SchemaAdjustmentSettings.DEFAULT;

  public boolean isRawInput(TableInput table) {
    //TODO: support other flexible formats
    return table.getParser() instanceof TextLineFormat.Parser;
  }

  @Override
  public StreamHolder<SourceRecord.Raw> getRawInput(TableInput table,
      StreamEngine.Builder builder, ErrorLocation errorLocation) {
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
    return textSource.mapWithError(new MapText2Raw(textparser), PARSE_ERROR_TAG,
        errorLocation, SourceRecord.Raw.class);
  }

  @AllArgsConstructor
  private static class MapText2Raw implements
      FunctionWithError<TimeAnnotatedRecord<String>, SourceRecord.Raw> {

    private final TextLineFormat.Parser textparser;

    @Override
    public Optional<SourceRecord.Raw> apply(TimeAnnotatedRecord<String> t,
        Supplier<ErrorCollector> errorCollector) {
      Format.Parser.Result r = textparser.parse(t.getRecord());
      if (r.isSuccess()) {
        Instant sourceTime = r.getSourceTime();
        if (sourceTime == null) {
          sourceTime = t.getSourceTime();
        }
        return Optional.of(new SourceRecord.Raw(r.getRecord(), sourceTime));
      } else {
        errorCollector.get().fatal(r.getErrorMsg());
        return Optional.empty();
      }
    }
  }

}
