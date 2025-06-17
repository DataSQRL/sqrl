/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Value;

/**
 * Represents a parsed object and keeps track of the position in the script so that we can produce
 * errors that point back to the source.
 *
 * @param <O>
 */
@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ParsedObject<O> {

  @Include O object;
  FileLocation fileLocation;

  public O get() {
    return object;
  }

  public <T> ParsedObject<T> map(Function<O, T> mapper) {
    if (object == null) {
      return new ParsedObject<>(null, fileLocation);
    }
    try {
      return new ParsedObject<>(mapper.apply(object), fileLocation);
    } catch (Exception e) {
      throw new StatementParserException(fileLocation, e);
    }
  }

  public <T> ParsedObject<T> fromOffset(ParsedObject<T> other) {
    return new ParsedObject<>(other.object, fileLocation.add(other.fileLocation));
  }

  public boolean isPresent() {
    return !isEmpty();
  }

  public boolean isEmpty() {
    return object == null;
  }
}
