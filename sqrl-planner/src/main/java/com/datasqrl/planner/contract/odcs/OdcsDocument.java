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
package com.datasqrl.planner.contract.odcs;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.InputFormat;
import com.networknt.schema.serialization.NodeReader;
import com.networknt.schema.utils.JsonNodes;
import java.io.IOException;

/**
 * A contract as it is read from its YAML file, in which every value knows its line and column, so
 * that an error on a value can be reported where the file has it.
 */
public record OdcsDocument(JsonNode root) {

  private static final NodeReader READER = NodeReader.builder().locationAware().build();

  /**
   * @param yaml the content of a YAML file with one document
   * @throws IOException if the content is not valid YAML
   */
  public static OdcsDocument read(String yaml) throws IOException {
    return new OdcsDocument(READER.readTree(yaml, InputFormat.YAML));
  }

  /**
   * @param pointer the JSON pointer of a value, empty for the root
   * @return the location of the value, or of its nearest parent that the document contains
   */
  public FileLocation locate(String pointer) {
    var current = pointer;
    while (true) {
      var node = root.at(current);
      var location = node.isMissingNode() ? null : JsonNodes.tokenStreamLocationOf(node);
      if (location != null) {
        return new FileLocation(location.getLineNr(), location.getColumnNr());
      }
      var lastSegment = current.lastIndexOf('/');
      if (lastSegment < 0) {
        return FileLocation.START;
      }
      current = current.substring(0, lastSegment);
    }
  }
}
