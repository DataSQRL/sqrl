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

import com.datasqrl.io.tables.TableType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.contract.ContractSubject;
import com.datasqrl.planner.contract.ContractSubject.Column;
import com.datasqrl.planner.contract.ContractSubject.ColumnKind;
import com.datasqrl.planner.contract.odcs.model.OdcsContract;
import com.datasqrl.planner.contract.odcs.model.OdcsProperty;
import com.datasqrl.planner.contract.odcs.model.OdcsSchemaObject;
import com.datasqrl.planner.hint.DocStringParser;
import com.datasqrl.planner.tables.StorageType;
import com.datasqrl.planner.tags.TableTags;
import com.datasqrl.planner.tags.TagDefinition;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.util.JsonSchemaValidator;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class OdcsGeneratorTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(OdcsGeneratorTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final RelDataType BIGINT_NOT_NULL = TYPES.createSqlType(SqlTypeName.BIGINT);

  private static final RelDataType STRING =
      TYPES.createTypeWithNullability(
          TYPES.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE), true);

  private static final RelDataType TIMESTAMP_NOT_NULL =
      TYPES.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);

  private static final TagDefinitions DEFINITIONS =
      TagDefinitions.of(
          List.of(
              TagDefinition.of("classification", List.of("public", "secret"), List.of()),
              TagDefinition.of("pii", List.of("masked", "raw"), List.of()),
              TagDefinition.of("gdpr", List.of(), List.of())));

  private final OdcsGenerator generator = new OdcsGenerator(DEFINITIONS);

  @Test
  void givenPhysicalMetadataAndComputedColumns_whenGenerate_thenOnlyPhysicalOnesAppear() {
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("loaded_at", TIMESTAMP_NOT_NULL, ColumnKind.COMPUTED),
                    new Column("event_time", TIMESTAMP_NOT_NULL, ColumnKind.METADATA),
                    new Column("email", STRING, ColumnKind.PHYSICAL)))
            .watermarkColumn(Optional.of("event_time"))
            .build();

    var schemaObject = generator.generate(subject);

    assertThat(schemaObject.properties())
        .extracting(OdcsProperty::name, OdcsProperty::logicalType, OdcsProperty::required)
        .containsExactly(tuple("id", "integer", true), tuple("email", "string", null));
    assertValidOdcs(schemaObject);
  }

  @Test
  void givenPhysicalWatermarkColumn_whenGenerate_thenItIsGenerated() {
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("received_at", TIMESTAMP_NOT_NULL, ColumnKind.PHYSICAL)))
            .watermarkColumn(Optional.of("received_at"))
            .build();

    var schemaObject = generator.generate(subject);

    assertThat(schemaObject.properties())
        .extracting(OdcsProperty::name)
        .containsExactly("id", "received_at");
  }

  @Test
  void givenKeys_whenGenerate_thenSetsTheirPositions() {
    var subject =
        subject("Orders")
            .columns(
                List.of(
                    new Column("region", STRING, ColumnKind.PHYSICAL),
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("amount", STRING, ColumnKind.PHYSICAL)))
            .primaryKey(List.of("id", "region"))
            .partitionKeys(List.of("REGION"))
            .build();

    var schemaObject = generator.generate(subject);

    assertThat(schemaObject.properties())
        .extracting(
            OdcsProperty::name,
            OdcsProperty::primaryKey,
            OdcsProperty::primaryKeyPosition,
            OdcsProperty::partitioned,
            OdcsProperty::partitionKeyPosition)
        .containsExactly(
            tuple("region", true, 2, true, 1),
            tuple("id", true, 1, null, null),
            tuple("amount", null, null, null, null));
    assertValidOdcs(schemaObject);
  }

  @Test
  void givenDocsAndTags_whenGenerate_thenSetsDescriptionsTagsAndClassification() {
    var documentation =
        DocStringParser.parse(
            "Customer records @pii=raw @classification=public\n"
                + "# Columns:\n"
                + "- email: The contact email @gdpr @classification=secret");
    var subject =
        subject("Customers")
            .columns(
                List.of(
                    new Column("id", BIGINT_NOT_NULL, ColumnKind.PHYSICAL),
                    new Column("email", STRING, ColumnKind.PHYSICAL)))
            .documentation(documentation)
            .tags(tags("@pii=raw @classification=secret @gdpr"))
            .storageType(Optional.of(StorageType.TOPIC))
            .storageName(Optional.of("customers-v1"))
            .build();

    var schemaObject = generator.generate(subject);

    assertThat(schemaObject)
        .extracting(
            OdcsSchemaObject::name,
            OdcsSchemaObject::physicalType,
            OdcsSchemaObject::physicalName,
            OdcsSchemaObject::logicalType,
            OdcsSchemaObject::description,
            OdcsSchemaObject::tags)
        .containsExactly(
            "Customers",
            "topic",
            "customers-v1",
            "object",
            "Customer records @pii=raw @classification=public",
            List.of("classification=secret", "gdpr", "pii=raw"));
    assertThat(schemaObject.properties())
        .extracting(
            OdcsProperty::name,
            OdcsProperty::description,
            OdcsProperty::tags,
            OdcsProperty::classification)
        .containsExactly(
            tuple("id", null, List.of(), "secret"),
            tuple(
                "email",
                "The contact email @gdpr @classification=secret",
                List.of("gdpr"),
                "secret"));
    assertValidOdcs(schemaObject);
  }

  @Test
  void givenDisabledTags_whenGenerate_thenNoTagsAndNoClassification() {
    var subject =
        subject("Customers")
            .columns(List.of(new Column("email", STRING, ColumnKind.PHYSICAL)))
            .documentation(DocStringParser.parse("Customers @pii=raw"))
            .build();

    var schemaObject = new OdcsGenerator(TagDefinitions.DISABLED).generate(subject);

    assertThat(schemaObject.tags()).isEmpty();
    assertThat(schemaObject.properties().get(0).tags()).isEmpty();
    assertThat(schemaObject.properties().get(0).classification()).isNull();
  }

  @Test
  void givenRowTypeOfComputedTable_whenGenerate_thenAllColumnsArePhysical() {
    var rowType =
        TYPES
            .builder()
            .add("customer_id", BIGINT_NOT_NULL)
            .add("num_orders", BIGINT_NOT_NULL)
            .build();
    var table =
        TableAnalysis.builder()
            .objectIdentifier(ObjectIdentifier.of("catalog", "database", "OrderCounts"))
            .type(TableType.STATE)
            .documentation(DocStringParser.parse("Orders per customer"))
            .tags(tags("@gdpr"))
            .build();
    var subject =
        ContractSubject.ofComputedTable("OrderCounts", rowType, table)
            .primaryKey(List.of("customer_id"))
            .storageType(Optional.of(StorageType.TABLE))
            .storageName(Optional.of("OrderCounts_1"))
            .build();

    var schemaObject = generator.generate(subject);

    assertThat(schemaObject.properties())
        .extracting(OdcsProperty::name, OdcsProperty::primaryKeyPosition)
        .containsExactly(tuple("customer_id", 1), tuple("num_orders", null));
    assertThat(schemaObject.tags()).containsExactly("gdpr");
    assertThat(schemaObject.description()).isEqualTo("Orders per customer");
    assertThat(schemaObject.physicalName()).isEqualTo("OrderCounts_1");
    assertThat(schemaObject.physicalType()).isEqualTo("table");
    assertValidOdcs(schemaObject);
  }

  private static ContractSubject.ContractSubjectBuilder subject(String name) {
    return ContractSubject.builder().name(name).columns(List.of());
  }

  private static TableTags tags(String tags) {
    return DEFINITIONS.parse(tags, invalidTag -> {});
  }

  /** Asserts that a contract with the schema object is valid ODCS. */
  @SneakyThrows
  static void assertValidOdcs(OdcsSchemaObject schemaObject) {
    var contract =
        new OdcsContract(
            OdcsVersion.API_VERSION,
            OdcsConstants.KIND,
            "test",
            "test",
            OdcsConstants.GENERATED_VERSION,
            List.of(OdcsYaml.toJson(schemaObject)));

    assertThat(
            JsonSchemaValidator.validate(
                OdcsYaml.toJson(contract), OdcsConstants.JSON_SCHEMA_RESOURCE))
        .isEmpty();
  }
}
