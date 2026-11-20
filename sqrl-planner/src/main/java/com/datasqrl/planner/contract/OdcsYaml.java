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

import com.datasqrl.planner.contract.model.OdcsSchemaObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Reads and writes data contracts as YAML. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class OdcsYaml {

  private static final YAMLMapper MAPPER =
      YAMLMapper.builder()
          .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
          .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES)
          .enable(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS)
          .build();

  /**
   * @return the documents of the YAML content, which is more than one if the content separates
   *     documents with {@code ---}
   * @throws IOException if the content is not valid YAML
   */
  public static List<JsonNode> readDocuments(String yaml) throws IOException {
    var documents = new ArrayList<JsonNode>();
    // Not ObjectReader.readValues, which reads the items of a list at the root as documents
    try (var parser = MAPPER.createParser(yaml)) {
      while (parser.nextToken() != null) {
        documents.add(MAPPER.readTree(parser));
      }
    }
    return documents;
  }

  /**
   * @param schemaObject a schema object of a contract file that is valid ODCS
   * @return the fields of the schema object that the compiler compares
   */
  public static OdcsSchemaObject toSchemaObject(JsonNode schemaObject) {
    try {
      return MAPPER.treeToValue(schemaObject, OdcsSchemaObject.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Not a schema object of a valid contract", e);
    }
  }

  /** Converts a generated model object to JSON, as a provided contract is read. */
  public static JsonNode toJson(Object model) {
    return MAPPER.valueToTree(model);
  }

  public static String write(Object contract) throws IOException {
    return MAPPER.writeValueAsString(contract);
  }
}
