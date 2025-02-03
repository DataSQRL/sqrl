package com.datasqrl.v2.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Value;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ParsedObject<O> {

  @Include
  O object;
  FileLocation fileLocation;

  public O get() {
    return object;
  }

  public<T> ParsedObject<T> map(Function<O,T> mapper) {
    if (object == null) return new ParsedObject<>(null, fileLocation);
    try {
      return new ParsedObject<>(mapper.apply(object), fileLocation);
    } catch (Exception e) {
      throw new StatementParserException(fileLocation, e);
    }
  }

  public<T> ParsedObject<T> fromOffset(ParsedObject<T> other) {
    return new ParsedObject<>(other.object, fileLocation.add(other.fileLocation));
  }

  public boolean isPresent() {
    return !isEmpty();
  }

  public boolean isEmpty() {
    return object == null;
  }

}
