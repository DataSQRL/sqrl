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
package com.datasqrl.planner.contract;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * The locations of the values of a YAML document by their JSON pointer, so that an error on a value
 * of the parsed document can be reported at its line and column.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class YamlLocations {

  private static final YAMLFactory YAML_FACTORY = new YAMLFactory();

  private final Map<String, FileLocation> locationsByPointer;

  /**
   * @param yaml the content of a YAML file; only its first document is read
   * @throws IOException if the content is not valid YAML
   */
  public static YamlLocations of(String yaml) throws IOException {
    var locations = new HashMap<String, FileLocation>();

    try (var parser = YAML_FACTORY.createParser(yaml)) {
      var depth = 0;
      for (var token = parser.nextToken(); token != null; token = parser.nextToken()) {
        if (token == JsonToken.FIELD_NAME) {
          continue;
        }
        if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
          if (--depth == 0) {
            break; // the end of the first document
          }
          continue;
        }

        var location = parser.currentTokenLocation();
        locations.putIfAbsent(
            parser.getParsingContext().pathAsPointer().toString(),
            new FileLocation(location.getLineNr(), location.getColumnNr()));

        if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
          depth++;
        } else if (depth == 0) {
          break; // a document that is a single scalar
        }
      }
    }

    return new YamlLocations(locations);
  }

  /**
   * @param pointer the JSON pointer of a value, empty for the root
   * @return the location of the value, or of its nearest parent that the document contains
   */
  public FileLocation locate(String pointer) {
    var current = pointer;
    while (true) {
      var location = locationsByPointer.get(current);
      if (location != null) {
        return location;
      }

      var lastSegment = current.lastIndexOf('/');
      if (lastSegment < 0) {
        return FileLocation.START;
      }
      current = current.substring(0, lastSegment);
    }
  }
}
