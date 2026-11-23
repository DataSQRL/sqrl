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

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.planner.contract.ContractSubject;
import com.datasqrl.planner.contract.ContractSubject.Column;
import com.datasqrl.planner.contract.odcs.model.OdcsProperty;
import com.datasqrl.planner.contract.odcs.model.OdcsSchemaObject;
import com.datasqrl.planner.tables.StorageType;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.planner.tags.TagValue;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;

/**
 * Generates the schema object of a data contract for a table. A contract contains the physical
 * columns of the table: metadata and computed columns are not part of it.
 */
@RequiredArgsConstructor
public class OdcsGenerator {

  private final TagDefinitions tagDefinitions;

  public OdcsSchemaObject generate(ContractSubject subject) {
    var walker = new OdcsPropertyWalker(subject.documentation().getColumnLookup());
    var properties =
        subject.physicalColumns().stream()
            .flatMap(
                column ->
                    walker
                        .walk(column.type(), NamePath.of(column.name()))
                        .map(property -> describe(property, column, subject))
                        .stream())
            .toList();

    return OdcsSchemaObject.builder()
        .name(subject.name())
        .physicalName(subject.storageName().orElse(null))
        .physicalType(subject.storageType().map(StorageType::getDisplayName).orElse(null))
        .logicalType(OdcsSchemaObject.LOGICAL_TYPE)
        .description(subject.documentation().docString())
        .tags(OdcsTags.write(subject.tags()))
        .properties(properties)
        .build();
  }

  /** Adds what the table declares for the column to the property of its type. */
  private OdcsProperty describe(OdcsProperty property, Column column, ContractSubject subject) {
    var description = subject.documentation().getColumnOpt(column.name());
    // The tags of a column are the tags in its own documentation
    var columnTags = tagDefinitions.parse(description.orElse(null), invalidTag -> {});
    var primaryKeyPosition = position(subject.primaryKey(), column.name());
    var partitionKeyPosition = position(subject.partitionKeys(), column.name());

    return property.toBuilder()
        .name(column.name())
        .description(description.orElse(null))
        .primaryKey(primaryKeyPosition.isPresent() ? Boolean.TRUE : null)
        .primaryKeyPosition(primaryKeyPosition.orElse(null))
        .partitioned(partitionKeyPosition.isPresent() ? Boolean.TRUE : null)
        .partitionKeyPosition(partitionKeyPosition.orElse(null))
        .classification(
            columnTags
                .get(OdcsConstants.CLASSIFICATION_TAG)
                .or(() -> subject.tags().get(OdcsConstants.CLASSIFICATION_TAG))
                .map(TagValue::value)
                .orElse(null))
        // The classification field of the property says what the classification tag says
        .tags(OdcsTags.write(columnTags.without(Set.of(OdcsConstants.CLASSIFICATION_TAG))))
        .build();
  }

  /** The position of the column in the key, starting at 1. */
  private static Optional<Integer> position(List<String> keyColumns, String column) {
    for (var i = 0; i < keyColumns.size(); i++) {
      if (keyColumns.get(i).equalsIgnoreCase(column)) {
        return Optional.of(i + 1);
      }
    }
    return Optional.empty();
  }
}
