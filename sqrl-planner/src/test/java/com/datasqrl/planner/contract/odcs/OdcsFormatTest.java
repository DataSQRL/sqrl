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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.planner.contract.DataContractFile;
import com.datasqrl.planner.contract.DataContractFormat;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OdcsFormatTest {

  private static final Path PATH = Path.of("crm.odcs.yaml");

  private static final String CONTRACT =
      """
      apiVersion: v3.2.0
      kind: DataContract
      id: crm
      version: 1.0.0
      schema:
        - name: Customers
          properties:
            - name: id
              logicalType: integer
        - name: Orders
      """;

  /** A format reports every error of a file, so the collector does not stop at the first */
  private final ErrorCollector errors = ErrorCollector.root().abortOnFatal(false);

  private final OdcsFormat underTest = new OdcsFormat();

  @Test
  void givenFormats_whenAll_thenOdcsIsOneOfThem() {
    assertThat(DataContractFormat.all())
        .extracting(DataContractFormat::getFileExtension)
        .containsExactly("odcs.yaml");
    assertThat(underTest.fileName("crm")).isEqualTo("crm.odcs.yaml");
    assertThat(underTest.getUnsupportedExtensions()).contains("odcs.yml", "odcs.json");
  }

  @Test
  void givenContract_whenRead_thenItsSchemaObjectsAreTheEntries() {
    var contractFile = read(CONTRACT).orElseThrow();

    assertThat(contractFile.getPath()).isEqualTo(PATH);
    assertThat(contractFile.getFormat()).isSameAs(underTest);
    assertThat(contractFile.getUnappliedEntryNames()).containsExactly("Customers", "Orders");
    assertThat(contractFile.getEntry("ORDERS").orElseThrow())
        .extracting(DataContractFormat.Entry::name, entry -> entry.location().line())
        .containsExactly("Orders", 10);
    assertThat(contractFile.getEntry("Unknown")).isEmpty();
    assertThat(contractFile.declaresTags()).isFalse();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenTagsOrClassification_whenRead_thenTheFileDeclaresTags() {
    assertThat(read(CONTRACT + "    tags: [gdpr]\n").orElseThrow().declaresTags()).isTrue();
    assertThat(
            read(CONTRACT.replace(
                    "        logicalType: integer\n",
                    "        logicalType: integer\n        classification: public\n"))
                .orElseThrow()
                .declaresTags())
        .isTrue();
  }

  @Test
  void givenYamlSyntaxError_whenRead_thenReportsItOnceWithItsLine() {
    var contractFile = read("apiVersion: v3.2.0\nschema:\n  - name: [unclosed\n");

    assertThat(contractFile).isEmpty();
    assertThat(messages()).hasSize(1);
    assertThat(messages().get(0).getMessage()).startsWith("Invalid YAML:");
    assertThat(messages().get(0).getLocation().getFileLocation().line()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void givenSchemaViolations_whenRead_thenReportsEachWithItsLine() {
    var contractFile =
        read(
            """
            apiVersion: v3.2.0
            kind: DataContract
            id: crm
            version: 1.0
            schema:
              - name: Customers
                columns: []
                properties:
                  - name: id
                    logicalType: int
                  - name: email
                    required: "yes"
            """);

    assertThat(contractFile).isEmpty();
    assertThat(messages())
        .extracting(message -> message.getLocation().getFileLocation().line())
        .contains(4, 10, 12);
    assertThat(messages())
        .filteredOn(message -> message.getLocation().getFileLocation().line() == 4)
        .extracting(ErrorMessage::getMessage)
        .allSatisfy(message -> assertThat(message).endsWith("Quote the value to make it a string"));
  }

  @Test
  void givenSchemaThatIsNotAList_whenRead_thenReportsItWithoutFailing() {
    var contractFile =
        read(
            """
            apiVersion: v3.2.0
            kind: DataContract
            id: crm
            version: 1.0.0
            status: active
            schema:
              name: Customers
            """);

    assertThat(contractFile).isEmpty();
    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(tuple("schema: object found, array expected", 7));
  }

  @Test
  void givenContractThatIsAList_whenRead_thenSaysWhatAContractIs() {
    assertThat(read("- name: Customers\n")).isEmpty();

    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "The contract file does not hold a data contract: it must be a YAML mapping with the"
                + " fields of ODCS, such as apiVersion and schema");
  }

  @Test
  void givenEmptyFile_whenRead_thenReportsIt() {
    assertThat(read("# no contract yet\n")).isEmpty();

    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly("The contract file is empty");
  }

  @Test
  void givenFieldThatOdcsDoesNotDefine_whenRead_thenReportsOnlyThatField() {
    // The validator reports every field of an object with an unknown field as not evaluated
    read(CONTRACT.replace("        logicalType: integer\n", "        nullable: true\n"));

    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(tuple("nullable: not a field that ODCS defines for this element", 9));
  }

  @Test
  void givenViolationsInTheSecondAndTwelfthProperty_whenRead_thenReportsBoth() {
    var properties = new StringBuilder();
    for (var i = 0; i < 11; i++) {
      properties.append("      - name: column").append(i).append("\n");
    }
    // The second property has a field of another element, which is only reported as not
    // evaluated. The violation of the twelfth property, whose pointer starts alike, is no reason
    // to leave that out
    read(
        CONTRACT
            .replace(
                "        logicalType: integer\n", "        logicalType: integer\n" + properties)
            .replace("      - name: column0\n", "      - name: column0\n        kind: Customer\n")
            .replace(
                "      - name: column10\n", "      - name: column10\n        logicalType: int\n"));

    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .anySatisfy(message -> assertThat(message).startsWith("kind: "))
        .anySatisfy(message -> assertThat(message).startsWith("logicalType: "));
  }

  @Test
  void givenContractOfAnOlderVersion_whenRead_thenAsksToConvertIt() {
    var contractFile = read(CONTRACT.replace("v3.2.0", "v3.1.0"));

    assertThat(contractFile).isEmpty();
    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(
            tuple("apiVersion [v3.1.0] is not supported: convert the contract to ODCS v3.2.0", 1));
  }

  @Test
  void givenContractWithoutVersion_whenRead_thenAsksForIt() {
    var contractFile = read(CONTRACT.replace("apiVersion: v3.2.0\n", ""));

    assertThat(contractFile).isEmpty();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .contains("The contract has no apiVersion: set it to v3.2.0");
  }

  @Test
  void givenSchemaObjectsDifferingInCase_whenRead_thenReportsTheSecond() {
    var contractFile = read(CONTRACT + "  - name: orders\n");

    assertThat(contractFile).isEmpty();
    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(
            tuple(
                "Schema objects [Orders] and [orders] differ only in case. Table names are"
                    + " matched ignoring case",
                11));
  }

  @Test
  void givenTwoYamlDocuments_whenRead_thenReportsIt() {
    var contractFile = read(CONTRACT + "---\n" + CONTRACT);

    assertThat(contractFile).isEmpty();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "The contract file has 2 YAML documents. A contract file holds one document");
  }

  private Optional<DataContractFile> read(String content) {
    return underTest.read(PATH, content, errors.withScript(PATH, content));
  }

  private List<ErrorMessage> messages() {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }
}
