package com.datasqrl;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.formats.FormatFactory.Parser;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.engine.stream.MapText2Raw;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import lombok.NonNull;
import lombok.Value;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

public interface FlinkFormatFactory {

  FunctionWithError<TimeAnnotatedRecord<String>, SourceRecord.Raw> create(SingleOutputStreamOperator<TimeAnnotatedRecord<String>> stream,
      OutputTag<InputError> createErrorTag);

  @Value
  class FlinkTextLineFormat implements FlinkFormatFactory {

    @NonNull TextLineFormat.Parser parser;

    @Override
    public FunctionWithError<TimeAnnotatedRecord<String>, Raw> create(
        SingleOutputStreamOperator<TimeAnnotatedRecord<String>> stream,
        OutputTag<InputError> createErrorTag) {
      MapText2Raw mapText2Raw = new MapText2Raw(parser);
      return mapText2Raw;
    }
  }

  static FlinkFormatFactory of(Parser parser) {
    if (parser instanceof TextLineFormat.Parser) {
      return new FlinkTextLineFormat((TextLineFormat.Parser)parser);
    } else {
      throw new NotYetImplementedException("Format not yet supported in data stream: " + parser.getClass());
    }
  }

}
