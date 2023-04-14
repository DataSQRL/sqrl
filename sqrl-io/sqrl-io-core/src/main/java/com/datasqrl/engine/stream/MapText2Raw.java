package com.datasqrl.engine.stream;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MapText2Raw implements
    FunctionWithError<TimeAnnotatedRecord<String>, Raw> {

  private final TextLineFormat.Parser textparser;

  @Override
  public Optional<Raw> apply(TimeAnnotatedRecord<String> t,
      Supplier<ErrorCollector> errorCollector) {
    Format.Parser.Result r = textparser.parse(t.getRecord());
    if (r.isSuccess()) {
      Instant sourceTime = r.getSourceTime();
      if (sourceTime == null) {
        sourceTime = t.getSourceTime();
      }
      return Optional.of(new Raw(r.getRecord(), sourceTime));
    } else {
      assert r.isError() || r.isSkip();
      if (r.isError()) {
        errorCollector.get().fatal(r.getErrorMsg());
      }
      return Optional.empty();
    }
  }
}
