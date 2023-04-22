package com.datasqrl;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.formats.FormatFactory.Parser;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.engine.stream.MapText2Raw;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

public interface FlinkFormatFactory {

  FunctionWithError<TimeAnnotatedRecord<String>, SourceRecord.Raw> create(Parser parser, SingleOutputStreamOperator<TimeAnnotatedRecord<String>> stream,
      OutputTag<InputError> createErrorTag);

  class FlinkTextLineFormat implements FlinkFormatFactory {

    @Override
    public FunctionWithError<TimeAnnotatedRecord<String>, Raw> create(Parser parser,
        SingleOutputStreamOperator<TimeAnnotatedRecord<String>> stream,
        OutputTag<InputError> createErrorTag) {
      if (parser instanceof TextLineFormat.Parser) {
        //wrap in error
        MapText2Raw mapText2Raw = new MapText2Raw((TextLineFormat.Parser)parser);
        return mapText2Raw;
      } else {
        throw new UnsupportedOperationException("Unknown format type");
      }
    }
  }
}
