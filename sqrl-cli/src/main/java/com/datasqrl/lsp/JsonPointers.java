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
package com.datasqrl.lsp;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;

/**
 * Finds the values of package files that the messages of their validation are about, e.g. {@code
 * /enabled-engines/4} of {@code ... at location [/enabled-engines/4]}.
 */
final class JsonPointers {

  private static final Pattern LOCATION = Pattern.compile("at location \\[(/[^\\]]*)]");

  private static final JsonFactory JSON = new JsonFactory();

  /** A range in a file. */
  record FileRange(Path file, Range range) {}

  private JsonPointers() {}

  /**
   * The value that a message about a package file is about, in the last of the package files that
   * has it, since later package files override earlier ones.
   */
  static Optional<FileRange> locate(
      String message, List<Path> packageFiles, Function<Path, Optional<String>> read) {
    var matcher = LOCATION.matcher(message);
    if (!matcher.find()) {
      return Optional.empty();
    }
    var pointer = matcher.group(1);
    for (var i = packageFiles.size() - 1; i >= 0; i--) {
      var file = packageFiles.get(i);
      var range = read.apply(file).flatMap(json -> find(json, pointer));
      if (range.isPresent()) {
        return Optional.of(new FileRange(file, range.get()));
      }
    }
    return Optional.empty();
  }

  /**
   * The range of the value at a JSON pointer, or of the name of an object or array value, which
   * spans several lines.
   */
  static Optional<Range> find(String json, String pointer) {
    try (var parser = JSON.createParser(json)) {
      JsonLocation name = null;
      String fieldName = null;
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        if (token == JsonToken.FIELD_NAME) {
          name = parser.currentTokenLocation();
          fieldName = parser.currentName();
          continue;
        } else if (token.isStructEnd()) {
          continue;
        }
        var context =
            token.isStructStart()
                ? parser.getParsingContext().getParent()
                : parser.getParsingContext();
        if (!context.pathAsPointer().toString().equals(pointer)) {
          continue;
        }
        if (token.isStructStart() && name != null && context.inObject()) {
          // The quoted name of an object or array value
          return Optional.of(range(name, fieldName.length() + 2));
        }
        var start = parser.currentTokenLocation();
        // Values are read lazily, so the location after the token is only known once it is read
        parser.finishToken();
        var end = parser.currentLocation();
        return Optional.of(
            new Range(position(start), token.isStructStart() ? position(start) : position(end)));
      }
    } catch (IOException e) {
      // An invalid package file has a message from its parser
    }
    return Optional.empty();
  }

  private static Range range(JsonLocation start, int length) {
    var from = position(start);
    return new Range(from, new Position(from.getLine(), from.getCharacter() + length));
  }

  /** The position of a location of the parser, whose lines and columns are 1-based. */
  private static Position position(JsonLocation location) {
    return new Position(location.getLineNr() - 1, location.getColumnNr() - 1);
  }
}
