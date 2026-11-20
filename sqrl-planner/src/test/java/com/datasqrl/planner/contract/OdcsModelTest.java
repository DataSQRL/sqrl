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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.planner.contract.model.OdcsContract;
import com.datasqrl.planner.contract.model.OdcsMap;
import com.datasqrl.planner.contract.model.OdcsProperty;
import com.datasqrl.planner.contract.model.OdcsSchemaObject;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class OdcsModelTest {

  @Test
  @SneakyThrows
  void givenContract_whenWrite_thenOmitsEmptyFieldsAndKeepsPropertyOrder() {
    var options = new LinkedHashMap<String, Object>();
    options.put("format", "i64");
    var id =
        OdcsProperty.builder()
            .name("id")
            .logicalType("integer")
            .logicalTypeOptions(options)
            .physicalType("BIGINT")
            .required(true)
            .primaryKey(true)
            .primaryKeyPosition(1)
            .tags(List.of())
            .build();
    var attributes =
        OdcsProperty.builder()
            .name("attributes")
            .logicalType("map")
            .map(
                new OdcsMap(
                    OdcsProperty.builder().logicalType("string").required(true).build(),
                    OdcsProperty.builder().logicalType("string").build()))
            .build();
    var table =
        OdcsSchemaObject.builder()
            .name("Customers")
            .physicalType("topic")
            .logicalType(OdcsSchemaObject.LOGICAL_TYPE)
            .tags(List.of("gdpr", "pii=raw"))
            .properties(List.of(id, attributes))
            .build();
    var contract =
        new OdcsContract(
            "v3.2.0",
            "DataContract",
            "sources",
            "sources",
            "1.0.0",
            List.of(OdcsYaml.toJson(table)));

    assertThat(OdcsYaml.write(contract))
        .isEqualTo(
            """
            apiVersion: v3.2.0
            kind: DataContract
            id: sources
            name: sources
            version: 1.0.0
            schema:
            - name: Customers
              physicalType: topic
              logicalType: object
              tags:
              - gdpr
              - pii=raw
              properties:
              - name: id
                logicalType: integer
                logicalTypeOptions:
                  format: i64
                physicalType: BIGINT
                required: true
                primaryKey: true
                primaryKeyPosition: 1
              - name: attributes
                logicalType: map
                map:
                  key:
                    logicalType: string
                    required: true
                  value:
                    logicalType: string
            """);
  }

  @Test
  @SneakyThrows
  void givenWrittenContract_whenReadDocuments_thenReturnsOneDocument() {
    var contract = new OdcsContract("v3.2.0", "DataContract", "sinks", "sinks", "1.0.0", List.of());

    var documents = OdcsYaml.readDocuments(OdcsYaml.write(contract));

    assertThat(documents).hasSize(1);
    assertThat(documents.get(0).get("id").asText()).isEqualTo("sinks");
    assertThat(documents.get(0).has("schema")).isFalse();
  }

  @Test
  @SneakyThrows
  void givenSeveralDocuments_whenReadDocuments_thenReturnsAllOfThem() {
    assertThat(OdcsYaml.readDocuments("a: 1\n---\nb: 2\n")).hasSize(2);
  }
}
