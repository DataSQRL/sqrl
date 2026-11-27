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

import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.planner.contract.ContractSubject;
import com.datasqrl.planner.contract.ContractSubject.Column;
import com.datasqrl.planner.contract.ContractSubject.ColumnKind;
import com.datasqrl.planner.contract.DataContractFile;
import com.datasqrl.planner.contract.DataContractLoader;
import com.datasqrl.planner.hint.DocStringParser;
import com.datasqrl.planner.tags.TagDefinition;
import com.datasqrl.planner.tags.TagDefinitions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OdcsCompatibilityCheckerTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(
          OdcsCompatibilityCheckerTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelDataType BIGINT_NOT_NULL = TYPES.createSqlType(SqlTypeName.BIGINT);
  private static final RelDataType STRING_NOT_NULL =
      TYPES.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
  private static final RelDataType STRING = TYPES.createTypeWithNullability(STRING_NOT_NULL, true);
  private static final RelDataType TIMESTAMP_NOT_NULL =
      TYPES.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);

  private static final TagDefinitions DEFINITIONS =
      TagDefinitions.of(
          List.of(
              TagDefinition.of("classification", List.of("public", "secret"), List.of()),
              TagDefinition.of("pii", List.of("masked", "raw"), List.of()),
              TagDefinition.of("gdpr", List.of(), List.of())));

  private static final String HEADER =
      """
      apiVersion: v3.2.0
      kind: DataContract
      id: crm
      version: 1.0.0
      schema:
        - name: Customers
      """;

  @TempDir Path folder;

  private final ErrorCollector errors = ErrorCollector.root();

  @Test
  void givenMatchingContractInAnotherOrderAndCase_whenCheck_thenCompatible() {
    var contract =
        HEADER
            + """
                  description: Anything goes here
                  properties:
                    - name: EMAIL
                      logicalType: string
                      physicalType: text
                      description: Not compared
                      logicalTypeOptions:
                        maxLength: 5
                    - name: id
                      logicalType: integer
                      required: true
                    - name: address
                      logicalType: object
                      properties:
                        - name: zip
                          logicalType: string
                        - name: city
                          logicalType: string
                          required: true
                    - name: labels
                      logicalType: array
                      items:
                        logicalType: string
              """;

    assertThat(check(contract, customers().build(), DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenMissingAndUnknownProperties_whenCheck_thenReportsEachAtItsLine() {
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: mail
                      logicalType: string
                    - name: address
                      logicalType: object
                      properties:
                        - name: city
                          logicalType: string
                          required: true
                        - name: country
                          logicalType: string
                    - name: labels
                      logicalType: array
                      items:
                        logicalType: string
              """;

    assertThat(check(contract, customers().build(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactlyInAnyOrder(
            tuple("Column [email] (STRING) of table [Customers] is missing from the contract", 6),
            tuple(
                "Contract property [mail] does not match a column of table [Customers]; columns"
                    + " are: id, email, address, labels",
                11),
            tuple(
                "Field [address.zip] (STRING) of table [Customers] is missing from the contract",
                13),
            tuple(
                "Contract property [address.country] does not match a field of [address]; fields"
                    + " are: city, zip",
                19));
  }

  @Test
  void givenDifferentTypesAndNullability_whenCheck_thenReportsEveryDifference() {
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: string
                      required: true
                    - name: email
                      logicalType: string
                      required: true
                    - name: address
                      logicalType: object
                      required: true
                      properties:
                        - name: city
                          logicalType: integer
                          required: true
                        - name: zip
                    - name: labels
                      logicalType: array
                      items:
                        logicalType: integer
                        required: true
              """;

    assertThat(check(contract, customers().build(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactlyInAnyOrder(
            tuple(
                "Column [id] has logicalType [string] in the contract, but the table has [integer]",
                9),
            tuple("required is true in the contract, but column [email] is nullable", 13),
            tuple("required is true in the contract, but column [address] is nullable", 16),
            tuple(
                "Field [address.city] has logicalType [integer] in the contract, but the table has"
                    + " [string]",
                19),
            tuple("Field [address.zip] has no logicalType in the contract, expected [string]", 21),
            tuple("required is true in the contract, but field [labels[]] is nullable", 26),
            tuple(
                "Field [labels[]] has logicalType [integer] in the contract, but the table has"
                    + " [string]",
                25));
  }

  @Test
  void givenArrayWithoutItems_whenCheck_thenReportsTheMissingDescription() {
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: email
                      logicalType: string
                    - name: address
                      logicalType: object
                    - name: labels
                      logicalType: array
              """;

    assertThat(check(contract, customers().build(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactlyInAnyOrder(
            "Field [address.city] (STRING NOT NULL) of table [Customers] is missing from the"
                + " contract",
            "Field [address.zip] (STRING) of table [Customers] is missing from the contract",
            "Field [labels[]] is not described in the contract");
  }

  private static final String ID_PROPERTY =
      """
            properties:
              - name: id
                logicalType: integer
                required: true
        """;

  @Test
  void givenMetadataWatermarkColumnInContract_whenCheck_thenCompatible() {
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("event_time", TIMESTAMP_NOT_NULL, ColumnKind.METADATA)))
            .watermarkColumn(Optional.of("event_time"))
            .build();
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: Event_Time
                      logicalType: timestamp
              """;

    assertThat(check(contract, subject, DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenWatermarkColumnThatTheTableAddsMissingFromContract_whenCheck_thenCompatible() {
    // A connector table that takes the columns of the data over with LIKE and adds the event time
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("received_at", TIMESTAMP_NOT_NULL, ColumnKind.PHYSICAL)))
            .watermarkColumn(Optional.of("received_at"))
            .addsWatermarkColumn(true)
            .build();

    assertThat(check(HEADER + ID_PROPERTY, subject, DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenWatermarkColumnThatTheTableDoesNotAddMissingFromContract_whenCheck_thenReportsIt() {
    // The table takes the column over with LIKE, or it has no LIKE clause: the column is data
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("received_at", TIMESTAMP_NOT_NULL, ColumnKind.PHYSICAL)))
            .watermarkColumn(Optional.of("received_at"))
            .build();

    assertThat(check(HEADER + ID_PROPERTY, subject, DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Column [received_at] (TIMESTAMP_LTZ(3) NOT NULL) of table [Customers] is missing from"
                + " the contract."
                + " It has the watermark, and a contract may only leave out a watermark column"
                + " that the table adds itself to the columns it takes over with LIKE");
  }

  @Test
  void givenMetadataAndComputedColumnsInContract_whenCheck_thenReportsThem() {
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("kafka_offset", BIGINT_NOT_NULL, ColumnKind.METADATA),
                    new Column("loaded_at", TIMESTAMP_NOT_NULL, ColumnKind.COMPUTED),
                    new Column("event_time", TIMESTAMP_NOT_NULL, ColumnKind.METADATA)))
            .watermarkColumn(Optional.of("event_time"))
            .build();
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: kafka_offset
                      logicalType: integer
                    - name: loaded_at
                      logicalType: timestamp
              """;

    assertThat(check(contract, subject, DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactly(
            tuple(
                "Contract property [kafka_offset] is a metadata column of table [Customers]. A"
                    + " contract contains physical columns only",
                11),
            tuple(
                "Contract property [loaded_at] is a computed column of table [Customers]. A"
                    + " contract contains physical columns only",
                13));
  }

  @Test
  void givenKeysOfTheTable_whenCheck_thenCompatibleWithAndWithoutPositions() {
    var subject =
        orders().primaryKey(List.of("id", "region")).partitionKeys(List.of("region")).build();
    var withPositions =
        HEADER.replace("Customers", "Orders")
            + """
                  properties:
                    - name: region
                      logicalType: string
                      required: true
                      primaryKey: true
                      primaryKeyPosition: 2
                      partitioned: true
                      partitionKeyPosition: 1
                    - name: id
                      logicalType: integer
                      required: true
                      primaryKey: true
                      primaryKeyPosition: 1
                    - name: amount
                      logicalType: string
              """;
    var withoutPositions =
        withPositions
            .replace("        primaryKeyPosition: 2\n", "")
            .replace("        primaryKeyPosition: 1\n", "");
    var withoutKeys =
        withoutPositions
            .replace("        primaryKey: true\n", "")
            .replace("        partitioned: true\n", "")
            .replace("        partitionKeyPosition: 1\n", "");

    assertThat(check(withPositions, subject, DEFINITIONS)).isTrue();
    assertThat(check(withoutPositions, subject, DEFINITIONS)).isTrue();
    assertThat(check(withoutKeys, subject, DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenOtherKeysThanTheTable_whenCheck_thenReportsThem() {
    var subject = orders().primaryKey(List.of("id", "region")).build();
    var contract =
        HEADER.replace("Customers", "Orders")
            + """
                  properties:
                    - name: region
                      logicalType: string
                      required: true
                      primaryKey: true
                      primaryKeyPosition: 1
                      partitioned: true
                    - name: id
                      logicalType: integer
                      required: true
                      primaryKey: true
                      primaryKeyPosition: 2
                    - name: amount
                      logicalType: string
              """;

    assertThat(check(contract, subject, DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactly(
            tuple(
                "The contract declares primary key (region, id) but table [Orders] has primary key"
                    + " (id, region)",
                11),
            tuple(
                "The contract declares partition key (region) but table [Orders] has no partition"
                    + " key",
                13));
  }

  @Test
  void givenTheTagsOfTheTableOnSchemaObject_whenCheck_thenCompatibleAndLabelsAreIgnored() {
    // As the compiler generates it, with labels that compiler.tags does not define
    var contract =
        HEADER
            + """
                  tags: ['PII = raw', gdpr, classification=secret, team=crm, golden]
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: email
                      logicalType: string
                      classification: Secret
                      tags: [gdpr, owner=sales]
              """;

    assertThat(check(contract, taggedCustomers(), DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenTheTagsOfTheTableOnProperties_whenCheck_thenCompatible() {
    // As a data catalog writes it: the classification and the tags on the columns they concern
    var contract =
        HEADER
            + """
                  tags: [pii=raw]
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: email
                      logicalType: string
                      classification: secret
                      tags: [gdpr]
              """;

    assertThat(check(contract, taggedCustomers(), DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenOtherTagsThanTheTable_whenCheck_thenReportsEveryDifference() {
    var contract =
        HEADER
            + """
                  tags: [pii=masked]
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                      classification: public
                    - name: email
                      logicalType: string
                      tags: [classification=top, pii]
              """;

    assertThat(check(contract, taggedCustomers(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactlyInAnyOrder(
            // The table has the classification of its column email, and id declares none
            tuple(
                "Property [id] declares classification [public], but column [id] of table"
                    + " [Customers] does not declare it and the table has [classification=secret]."
                    + " Declare it in the comment of the column, or list the classification of"
                    + " the table as a tag on the schema object",
                12),
            tuple(
                "classification [secret] of column [email] of table [Customers] is missing from"
                    + " property [email]",
                13),
            tuple(
                "Contract tag [classification=top] is not a valid value of tag [classification]:"
                    + " allowed values are [public, secret]",
                15),
            tuple(
                "Contract tag [pii] is not a valid value of tag [pii]: allowed values are [masked,"
                    + " raw]",
                15),
            tuple(
                "Tag [gdpr] of column [email] of table [Customers] is missing from property"
                    + " [email]",
                15),
            tuple("The contract declares tag [pii=masked] but table [Customers] has [pii=raw]", 7));
  }

  @Test
  void givenTagOnAnotherPropertyThanItsColumn_whenCheck_thenReportsBothProperties() {
    var contract =
        HEADER
            + """
                  tags: [pii=raw]
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                      tags: [gdpr]
                    - name: email
                      logicalType: string
                      classification: secret
              """;

    assertThat(check(contract, taggedCustomers(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactlyInAnyOrder(
            tuple(
                "Property [id] declares tag [gdpr], but column [id] of table [Customers] does not"
                    + " declare it. Declare it in the comment of the column, or list the tag of"
                    + " the table on the schema object",
                12),
            tuple(
                "Tag [gdpr] of column [email] of table [Customers] is missing from property"
                    + " [email]",
                13));
  }

  @Test
  void givenTagOfAColumnOnlyOnTheSchemaObject_whenCheck_thenReportsItsProperty() {
    var contract =
        HEADER
            + """
                  tags: [pii=raw, gdpr, classification=secret]
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: email
                      logicalType: string
              """;

    assertThat(check(contract, taggedCustomers(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactly(
            tuple(
                "classification [secret] of column [email] of table [Customers] is missing from"
                    + " property [email]",
                12),
            tuple(
                "Tag [gdpr] of column [email] of table [Customers] is missing from property"
                    + " [email]",
                12));
  }

  @Test
  void givenTagOfTheDocStringOnAProperty_whenCheck_thenReportsItOnce() {
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: email
                      logicalType: string
                      classification: secret
                      tags: [gdpr, pii=raw]
              """;

    assertThat(check(contract, taggedCustomers(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactly(
            tuple(
                "Property [email] declares tag [pii=raw], but column [email] of table [Customers]"
                    + " does not declare it. Declare it in the comment of the column, or list the"
                    + " tag of the table on the schema object",
                14));
  }

  @Test
  void givenTagsOnNestedProperties_whenCheck_thenTheyCountForTheirColumn() {
    var contact =
        TYPES.createFieldTypeFromLogicalType(
            new RowType(
                true,
                List.of(new RowField("email", new VarCharType(true, VarCharType.MAX_LENGTH)))));
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("contact", contact, ColumnKind.PHYSICAL)))
            .documentation(
                DocStringParser.parse(
                    "Customer records\n"
                        + "# Columns:\n"
                        + "- contact: How to reach the customer @gdpr @classification=secret"))
            .tags(DEFINITIONS.parse("@gdpr @classification=secret", invalidTag -> {}))
            .build();
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: contact
                      logicalType: object
                      properties:
                        - name: email
                          logicalType: string
                          classification: secret
                          tags: [gdpr]
              """;

    assertThat(check(contract, subject, DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenTagsThatTheTableDoesNotHave_whenCheck_thenReportsWhereTheyAreDeclared() {
    var contract =
        HEADER.replace("Customers", "Orders")
            + """
                  properties:
                    - name: region
                      logicalType: string
                      required: true
                      classification: public
                    - name: id
                      logicalType: integer
                      required: true
                    - name: amount
                      logicalType: string
                      tags: [gdpr]
              """;

    assertThat(check(contract, orders().build(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactlyInAnyOrder(
            tuple(
                "The contract declares tag [classification=public] that table [Orders] does not"
                    + " have",
                11),
            tuple("The contract declares tag [gdpr] that table [Orders] does not have", 17));
  }

  @Test
  void givenDisabledTags_whenCheck_thenTagsAndClassificationAreNotCompared() {
    var contract =
        HEADER
            + """
                  tags: [pii=masked]
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: email
                      logicalType: string
                      classification: public
                      tags: [gdpr]
              """;
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("email", STRING, ColumnKind.PHYSICAL)))
            .build();

    assertThat(check(contract, subject, TagDefinitions.DISABLED)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenPropertyDefinedTwice_whenCheck_thenReportsIt() {
    var contract =
        HEADER.replace("Customers", "Orders")
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: region
                      logicalType: string
                      required: true
                    - name: amount
                      logicalType: string
                    - name: Amount
                      logicalType: string
              """;

    assertThat(check(contract, orders().build(), DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage, OdcsCompatibilityCheckerTest::line)
        .containsExactly(
            tuple(
                "Contract property [Amount] names column [amount] of table [Orders], which another"
                    + " property of the contract names too",
                16));
  }

  @Test
  void givenColumnsThatDifferOnlyInCase_whenCheck_thenMatchesTheExactNames() {
    var subject =
        subject("Codes")
            .columns(
                List.of(
                    new Column("code", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("CODE", STRING, ColumnKind.PHYSICAL)))
            .build();
    var contract =
        HEADER.replace("Customers", "Codes")
            + """
                  properties:
                    - name: CODE
                      logicalType: string
                    - name: code
                      logicalType: integer
                      required: true
              """;

    assertThat(check(contract, subject, DEFINITIONS)).isTrue();
    assertThat(messages()).isEmpty();
  }

  @Test
  void givenPhysicalWatermarkColumnInContract_whenCheck_thenItIsComparedLikeAnyColumn() {
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("received_at", TIMESTAMP_NOT_NULL, ColumnKind.PHYSICAL)))
            .watermarkColumn(Optional.of("received_at"))
            .build();
    var contract =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: received_at
                      logicalType: string
                      required: true
              """;

    assertThat(check(contract, subject, DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Column [received_at] has logicalType [string] in the contract, but the table has"
                + " [timestamp]");
  }

  @Test
  void givenMap_whenCheck_thenComparesItsKeyAndValue() {
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column(
                        "attributes",
                        TYPES.createTypeWithNullability(
                            TYPES.createMapType(STRING_NOT_NULL, STRING), true),
                        ColumnKind.PHYSICAL)))
            .build();
    var asObject =
        HEADER
            + """
                  properties:
                    - name: id
                      logicalType: integer
                      required: true
                    - name: attributes
                      logicalType: object
              """;
    var asMap =
        asObject.replace("logicalType: object", "logicalType: map")
            + """
                      map:
                        key:
                          logicalType: string
                          required: true
                        value:
                          logicalType: integer
              """;

    assertThat(check(asObject, subject, DEFINITIONS)).isFalse();
    assertThat(check(asMap, subject, DEFINITIONS)).isFalse();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Column [attributes] has logicalType [object] in the contract, but the table has [map]",
            "Field [attributes.value] has logicalType [integer] in the contract, but the table has"
                + " [string]");
  }

  /** A table with a nested row and an array. */
  private static ContractSubject.ContractSubjectBuilder customers() {
    // A nullable row with a NOT NULL field, as the type of a table column is created
    var address =
        TYPES.createFieldTypeFromLogicalType(
            new RowType(
                true,
                List.of(
                    new RowField("city", new VarCharType(false, VarCharType.MAX_LENGTH)),
                    new RowField("zip", new VarCharType(true, VarCharType.MAX_LENGTH)))));

    return subject("Customers")
        .columns(
            List.of(
                new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                new Column("email", STRING, ColumnKind.PHYSICAL),
                new Column("address", address, ColumnKind.PHYSICAL),
                new Column(
                    "labels",
                    TYPES.createTypeWithNullability(TYPES.createArrayType(STRING, -1), true),
                    ColumnKind.PHYSICAL),
                new Column("loaded_at", TIMESTAMP_NOT_NULL, ColumnKind.COMPUTED)));
  }

  private static ContractSubject.ContractSubjectBuilder orders() {
    return subject("Orders")
        .columns(
            List.of(
                new Column("region", STRING_NOT_NULL, ColumnKind.PHYSICAL),
                new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                new Column("amount", STRING, ColumnKind.PHYSICAL)));
  }

  /** A table whose tags are the tags of its doc string and of the comment of a column. */
  private static ContractSubject taggedCustomers() {
    return subject("Customers")
        .columns(
            List.of(
                new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                new Column("email", STRING, ColumnKind.PHYSICAL)))
        .documentation(
            DocStringParser.parse(
                "Customer records @pii=raw\n"
                    + "# Columns:\n"
                    + "- email: The contact email @gdpr @classification=secret"))
        .tags(DEFINITIONS.parse("@pii=raw @gdpr @classification=secret", invalidTag -> {}))
        .build();
  }

  private static ContractSubject.ContractSubjectBuilder subject(String name) {
    return ContractSubject.builder().name(name).columns(List.of());
  }

  /**
   * Compares the contract of the table with the schema object generated for the table, and reports
   * the differences at their lines.
   *
   * @return whether the contract matches the table
   */
  private boolean check(String contract, ContractSubject subject, TagDefinitions definitions) {
    var file = load(contract);
    var entry = file.getEntry(subject.name()).orElseThrow();
    var differences = file.getFormat().compare(entry, subject, definitions);

    differences.forEach(
        difference -> file.errorsAt(difference.location()).fatal("%s", difference.message()));
    return differences.isEmpty();
  }

  private int contracts;

  @SneakyThrows
  private DataContractFile load(String contract) {
    var name = "crm" + contracts++;
    Files.writeString(folder.resolve(name + ".odcs.yaml"), contract);
    var script = folder.resolve(name + ".sqrl");
    Files.writeString(script, "");

    return new DataContractLoader(errors, new WorkspacePaths(folder, folder, folder, folder))
        .forScript(script)
        .orElseThrow(() -> new AssertionError("Invalid contract: " + messages()));
  }

  private List<ErrorMessage> messages() {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }

  private static int line(ErrorMessage message) {
    return message.getLocation().getFileLocation().line();
  }
}
