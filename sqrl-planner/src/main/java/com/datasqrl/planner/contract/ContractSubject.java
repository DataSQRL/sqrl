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

import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.contract.TableContracts.ContractTable;
import com.datasqrl.planner.tables.StorageType;
import com.datasqrl.planner.tags.TableTags;
import com.datasqrl.planner.util.Documented;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Builder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

/**
 * A table that a data contract describes, with what the contract is generated from and compared
 * with. Only the name and the columns are required.
 *
 * @param name the name of the table in its statement
 * @param columns all columns of the table, of which only the physical ones are part of a contract
 * @param watermarkColumn the top-level column that the watermark of the table is declared on
 * @param addsWatermarkColumn whether the statement of the table adds the watermark column itself to
 *     the columns that it takes over from another table with {@code LIKE}
 * @param primaryKey the declared primary key columns, in order
 * @param partitionKeys the declared partition columns, in order
 * @param tags the tags of the table
 * @param storageType the kind of object that stores the table, e.g. a topic
 * @param storageName the name of the object that stores the table
 */
@Builder
public record ContractSubject(
    String name,
    List<Column> columns,
    Optional<String> watermarkColumn,
    boolean addsWatermarkColumn,
    List<String> primaryKey,
    List<String> partitionKeys,
    Documentation documentation,
    TableTags tags,
    Optional<StorageType> storageType,
    Optional<String> storageName) {

  public enum ColumnKind {
    PHYSICAL,
    METADATA,
    COMPUTED
  }

  public record Column(String name, RelDataType type, ColumnKind kind) {}

  public ContractSubject {
    Objects.requireNonNull(name);
    Objects.requireNonNull(columns);
    watermarkColumn = Objects.requireNonNullElse(watermarkColumn, Optional.empty());
    primaryKey = Objects.requireNonNullElse(primaryKey, List.of());
    partitionKeys = Objects.requireNonNullElse(partitionKeys, List.of());
    documentation = Objects.requireNonNullElse(documentation, Documented.EMPTY);
    tags = Objects.requireNonNullElse(tags, TableTags.EMPTY);
    storageType = Objects.requireNonNullElse(storageType, Optional.empty());
    storageName = Objects.requireNonNullElse(storageName, Optional.empty());
  }

  /**
   * @param created a table that a {@code CREATE TABLE} statement defines
   * @param partitionKeys the partition columns that the statement declares
   * @param typeFactory converts the column types of the table
   */
  public static ContractSubject ofCreatedTable(
      ContractTable created, List<String> partitionKeys, FlinkTypeFactory typeFactory) {
    var table = created.table();
    var sourceSink = table.getSourceSinkTable().orElseThrow();
    var schema = sourceSink.schema();

    // The columns are read from the schema and not through the row type of the table, which
    // requires that computed columns come last
    var columns =
        schema.getColumns().stream()
            .map(
                column ->
                    new Column(
                        column.getName(),
                        typeFactory.createFieldTypeFromLogicalType(
                            column.getDataType().getLogicalType()),
                        kindOf(column)))
            .toList();

    var watermarkColumn =
        schema.getWatermarkSpecs().stream()
            .map(WatermarkSpec::getRowtimeAttribute)
            .filter(column -> !column.contains("."))
            .findFirst();

    return ContractSubject.builder()
        .name(created.name())
        .columns(columns)
        .watermarkColumn(watermarkColumn)
        .addsWatermarkColumn(
            watermarkColumn
                .filter(
                    column -> created.addedColumns().stream().anyMatch(column::equalsIgnoreCase))
                .isPresent())
        .primaryKey(schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(List.of()))
        .partitionKeys(partitionKeys)
        .documentation(created.documentation())
        .tags(table.getTags())
        .storageType(sourceSink.connectorConfig().getStorageType())
        .storageName(sourceSink.connectorConfig().getStorageName())
        .build();
  }

  /**
   * @param name the name of the table for the systems that read it
   * @param rowType the columns as they are stored, which are all physical
   * @param table the table that the pipeline computes
   * @return a builder with the columns, the documentation and the tags of the table
   */
  public static ContractSubjectBuilder ofComputedTable(
      String name, RelDataType rowType, TableAnalysis table) {
    return ContractSubject.builder()
        .name(name)
        .columns(
            rowType.getFieldList().stream()
                .map(field -> new Column(field.getName(), field.getType(), ColumnKind.PHYSICAL))
                .toList())
        .documentation(table.getDocumentation())
        .tags(table.getTags());
  }

  private static ColumnKind kindOf(org.apache.flink.table.catalog.Column column) {
    if (column instanceof ComputedColumn) {
      return ColumnKind.COMPUTED;
    }
    return column instanceof MetadataColumn ? ColumnKind.METADATA : ColumnKind.PHYSICAL;
  }

  /** The columns that are part of a contract. */
  public List<Column> physicalColumns() {
    return columns.stream().filter(column -> column.kind() == ColumnKind.PHYSICAL).toList();
  }

  public Optional<Column> getColumn(String name) {
    return columns.stream().filter(column -> column.name().equalsIgnoreCase(name)).findFirst();
  }

  public boolean isWatermarkColumn(String name) {
    return watermarkColumn.filter(column -> column.equalsIgnoreCase(name)).isPresent();
  }

  /**
   * A contract may leave out the watermark column if the table adds it itself to the columns of the
   * table it extends: those columns are the data, and how an environment provides the event time is
   * up to its connector table. A watermark column that the table takes over is data.
   */
  public boolean mayOmitColumn(String name) {
    return addsWatermarkColumn && isWatermarkColumn(name);
  }
}
