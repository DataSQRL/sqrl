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

import com.datasqrl.planner.contract.ContractSubject.ColumnKind;
import com.datasqrl.planner.contract.model.OdcsMap;
import com.datasqrl.planner.contract.model.OdcsProperty;
import com.datasqrl.planner.contract.model.OdcsSchemaObject;
import com.datasqrl.planner.tags.TableTags;
import com.datasqrl.planner.tags.TagDefinition;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.planner.tags.TagValue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * Compares the schema object that a contract file provides for a table with the schema object that
 * the compiler generates for it, and returns every difference.
 *
 * <p>The comparison covers what matters for the schema and for compliance:
 *
 * <ul>
 *   <li>The properties must be the physical columns of the table, as a set that ignores case and
 *       order. The column that the watermark is declared on is exempt: it may be missing from the
 *       contract, and the contract may list it when the table reads it from metadata or computes
 *       it, since environments differ in how they provide it.
 *   <li>{@code logicalType} and {@code required} must be equal, also in nested properties, array
 *       items, and map keys and values.
 *   <li>A primary key or partition key that the contract declares must be the key of the table.
 *   <li>The tags that are defined in {@code compiler.tags} must be the tags of the table. Tags
 *       count toward the whole table, so it does not matter whether the contract lists a tag on the
 *       schema object or on a property. The {@code classification} of a property is the tag of that
 *       name.
 *   <li>A {@code classification} that the contract gives for a property must be the one of the
 *       column.
 * </ul>
 *
 * Everything else is ignored, e.g. {@code physicalType}, descriptions and {@code
 * logicalTypeOptions}.
 */
@RequiredArgsConstructor
public class OdcsCompatibilityChecker {

  /**
   * @param pointer the JSON pointer of the element of the contract file that differs
   */
  public record Difference(String pointer, String message) {}

  private final TagDefinitions tagDefinitions;

  /**
   * @param provided the schema object of a contract file
   * @param pointer the JSON pointer of the schema object in its file
   * @param generated the schema object generated for the table, for the ODCS version of the
   *     contract file
   * @param subject the table
   * @return every difference between the provided schema object and the table
   */
  public List<Difference> compare(
      OdcsSchemaObject provided,
      String pointer,
      OdcsSchemaObject generated,
      ContractSubject subject) {
    var comparison = new Comparison(subject, locate(provided.properties(), pointer));
    comparison.columns(generated.properties(), pointer);
    comparison.key(
        "primary key",
        OdcsProperty::primaryKey,
        OdcsProperty::primaryKeyPosition,
        "primaryKey",
        subject.primaryKey());
    comparison.key(
        "partition key",
        OdcsProperty::partitioned,
        OdcsProperty::partitionKeyPosition,
        "partitioned",
        subject.partitionKeys());
    if (tagDefinitions.isEnabled()) {
      comparison.tags(provided.tags(), pointer);
    }
    return comparison.differences;
  }

  /** A property of the contract with its JSON pointer in the contract file. */
  private record Located(OdcsProperty property, String pointer) {

    String name() {
      return property.name();
    }
  }

  private static List<Located> locate(List<OdcsProperty> properties, String pointer) {
    return IntStream.range(0, properties.size())
        .mapToObj(
            i ->
                new Located(
                    properties.get(i), pointer + "/" + OdcsConstants.PROPERTIES_FIELD + "/" + i))
        .toList();
  }

  private class Comparison {

    private final ContractSubject subject;

    /** The top-level properties of the contract */
    private final List<Located> properties;

    private final List<Difference> differences = new ArrayList<>();

    /** The properties with a classification that does not differ from the one of their column */
    private final List<Located> classified = new ArrayList<>();

    Comparison(ContractSubject subject, List<Located> properties) {
      this.subject = subject;
      this.properties = properties;
    }

    void columns(List<OdcsProperty> columns, String pointer) {
      var matching = new Matching(properties, columns);
      matching.forEachMatch(
          (column, property) -> {
            element(property.property(), property.pointer(), column, column.name(), true);
            classification(property, column);
          });

      for (var column : matching.missing) {
        if (!subject.isWatermarkColumn(column.name())) {
          report(
              pointer,
              "Column [%s] (%s) of table [%s] is missing from the contract",
              column.name(),
              describeType(column),
              subject.name());
        }
      }
      for (var property : matching.unmatched) {
        unmatchedProperty(property, columns);
      }
    }

    private void unmatchedProperty(Located property, List<OdcsProperty> columns) {
      var column = subject.getColumn(property.name());
      if (column.isEmpty()) {
        report(
            property.pointer(),
            "Contract property [%s] does not match a column of table [%s]; columns are: %s",
            property.name(),
            subject.name(),
            names(columns));
      } else if (column.get().kind() == ColumnKind.PHYSICAL) {
        report(
            property.pointer(),
            "Contract property [%s] names column [%s] of table [%s], which another property of"
                + " the contract names too",
            property.name(),
            column.get().name(),
            subject.name());
      } else if (!subject.isWatermarkColumn(property.name())) {
        report(
            property.pointer(),
            "Contract property [%s] is a %s column of table [%s]. A contract contains physical"
                + " columns only",
            property.name(),
            column.get().kind() == ColumnKind.COMPUTED ? "computed" : "metadata",
            subject.name());
      }
    }

    /**
     * Compares a property, the items of an array, or the key or value of a map.
     *
     * @param path the path of the element in the table, e.g. {@code address.city}
     */
    private void element(
        OdcsProperty provided,
        String pointer,
        OdcsProperty generated,
        String path,
        boolean isColumn) {
      var label = (isColumn ? "column [" : "field [") + path + "]";
      if (provided.mustBeSet() != generated.mustBeSet()) {
        report(
            pointer + "/required",
            "required is %s in the contract, but %s is %s",
            provided.mustBeSet(),
            label,
            generated.mustBeSet() ? "NOT NULL" : "nullable");
      }

      var logicalType = generated.logicalType();
      if (logicalType == null) {
        return; // a type that has no logical type cannot be compared
      }
      if (provided.logicalType() == null) {
        report(
            pointer,
            "%s has no logicalType in the contract, expected [%s]",
            StringUtils.capitalize(label),
            logicalType);
        return;
      }
      if (!provided.logicalType().equalsIgnoreCase(logicalType)) {
        report(
            pointer + "/logicalType",
            "%s has logicalType [%s] in the contract, but the table has [%s]",
            StringUtils.capitalize(label),
            provided.logicalType(),
            logicalType);
        return; // nested differences would only repeat this one
      }

      switch (logicalType) {
        case OdcsPropertyWalker.OBJECT -> fields(provided, pointer, generated, path);
        case OdcsPropertyWalker.ARRAY ->
            nested(provided.items(), pointer + "/items", generated.items(), path + "[]");
        case OdcsPropertyWalker.MAP -> {
          var map = Optional.ofNullable(provided.map());
          nested(
              map.map(OdcsMap::key).orElse(null),
              pointer + "/map/key",
              generated.map().key(),
              path + ".key");
          nested(
              map.map(OdcsMap::value).orElse(null),
              pointer + "/map/value",
              generated.map().value(),
              path + ".value");
        }
        default -> {
          // a scalar has nothing nested
        }
      }
    }

    private void nested(
        OdcsProperty provided, String pointer, OdcsProperty generated, String path) {
      if (generated == null) {
        return;
      }
      if (provided == null) {
        report(pointer, "Field [%s] is not described in the contract", path);
        return;
      }
      element(provided, pointer, generated, path, false);
    }

    /** Compares the fields of a nested row, which must be the same set. */
    private void fields(
        OdcsProperty provided, String pointer, OdcsProperty generated, String path) {
      if (generated.properties().isEmpty()) {
        return; // a type without fixed fields, such as JSON
      }
      var matching = new Matching(locate(provided.properties(), pointer), generated.properties());
      matching.forEachMatch(
          (field, property) ->
              element(
                  property.property(),
                  property.pointer(),
                  field,
                  path + "." + field.name(),
                  false));

      for (var field : matching.missing) {
        report(
            pointer,
            "Field [%s.%s] (%s) of table [%s] is missing from the contract",
            path,
            field.name(),
            describeType(field),
            subject.name());
      }
      for (var property : matching.unmatched) {
        report(
            property.pointer(),
            "Contract property [%s.%s] does not match a field of [%s]; fields are: %s",
            path,
            property.name(),
            path,
            names(generated.properties()));
      }
    }

    /**
     * A contract may leave out a key. A key that it declares must be the key of the table: the same
     * columns, and the same order if the contract gives the position of every key column.
     *
     * @param field the name of the field that marks a key column in the contract
     */
    void key(
        String keyName,
        Function<OdcsProperty, Boolean> isKeyColumn,
        Function<OdcsProperty, Integer> position,
        String field,
        List<String> tableKey) {
      var keyProperties =
          properties.stream()
              .filter(property -> Boolean.TRUE.equals(isKeyColumn.apply(property.property())))
              .collect(Collectors.toCollection(ArrayList::new));
      if (keyProperties.isEmpty()) {
        return;
      }

      var hasPositions =
          keyProperties.stream().allMatch(property -> position.apply(property.property()) != null);
      if (hasPositions) {
        keyProperties.sort(Comparator.comparing(property -> position.apply(property.property())));
      }
      var contractKey = keyProperties.stream().map(Located::name).toList();
      var contractColumns = contractKey.stream().map(DataContractFile::key).toList();
      var tableColumns = tableKey.stream().map(DataContractFile::key).toList();

      var matches =
          hasPositions
              ? contractColumns.equals(tableColumns)
              : contractColumns.size() == tableColumns.size()
                  && tableColumns.containsAll(contractColumns);
      if (!matches) {
        report(
            keyProperties.get(0).pointer() + "/" + field,
            "The contract declares %s (%s) but table [%s] has %s",
            keyName,
            String.join(", ", contractKey),
            subject.name(),
            tableKey.isEmpty()
                ? "no " + keyName
                : keyName + " (" + String.join(", ", tableKey) + ")");
      }
    }

    /** A classification that the contract gives for a property must be the one of the column. */
    private void classification(Located property, OdcsProperty column) {
      var classification = property.property().classification();
      if (classification == null
          || tagDefinitions.get(OdcsConstants.CLASSIFICATION_TAG).isEmpty()) {
        return;
      }
      // A column without a classification is in a table without one, which the tags report
      if (column.classification() == null
          || classification.equalsIgnoreCase(column.classification())) {
        classified.add(property);
      } else {
        report(
            property.pointer() + "/" + OdcsConstants.CLASSIFICATION_FIELD,
            "classification is [%s] in the contract, but column [%s] of table [%s] has [%s]",
            classification,
            column.name(),
            subject.name(),
            column.classification());
      }
    }

    /**
     * Compares the tags of the contract that are defined in {@code compiler.tags} with the tags of
     * the table. Other tags of the contract are labels of the catalog.
     */
    void tags(List<String> schemaObjectTags, String pointer) {
      var tagsPointer = pointer + "/" + OdcsConstants.TAGS_FIELD;
      var declared = new DeclaredTags();
      declared.addAll(schemaObjectTags, tagsPointer);
      properties.forEach(
          property ->
              declared.addAll(
                  property.property().tags(), property.pointer() + "/" + OdcsConstants.TAGS_FIELD));
      classified.forEach(
          property ->
              declared.add(
                  OdcsConstants.CLASSIFICATION_TAG + "=" + property.property().classification(),
                  property.pointer() + "/" + OdcsConstants.CLASSIFICATION_FIELD));

      var contractTags = declared.toTags();
      for (var tag : subject.tags().values()) {
        var found = contractTags.get(tag.name());
        if (found.isEmpty()) {
          report(
              tagsPointer,
              "Tag [%s] of table [%s] is missing from the contract",
              tag.toContractTag(),
              subject.name());
        } else if (!found.get().equals(tag)) {
          report(
              declared.pointerOf(found.get()),
              "The contract declares tag [%s] but table [%s] has [%s]",
              found.get().toContractTag(),
              subject.name(),
              tag.toContractTag());
        }
      }
      for (var tag : contractTags.values()) {
        if (subject.tags().get(tag.name()).isEmpty()) {
          report(
              declared.pointerOf(tag),
              "The contract declares tag [%s] that table [%s] does not have",
              tag.toContractTag(),
              subject.name());
        }
      }
    }

    /** The tags of the contract that are defined in {@code compiler.tags}. */
    private class DeclaredTags {

      /** The pointer of the first declaration of each tag value */
      private final Map<TagValue, String> pointers = new LinkedHashMap<>();

      void addAll(List<String> tags, String pointer) {
        IntStream.range(0, tags.size()).forEach(i -> add(tags.get(i), pointer + "/" + i));
      }

      /**
       * @param tag a tag as a contract writes it: {@code name} or {@code name=value}
       */
      void add(String tag, String pointer) {
        var name = StringUtils.substringBefore(tag, "=").trim();
        var value = tag.contains("=") ? StringUtils.substringAfter(tag, "=").trim() : null;
        var definition = tagDefinitions.get(name);
        if (definition.isEmpty()) {
          return; // a label of the catalog
        }
        definition
            .get()
            .valueOf(value)
            .ifPresentOrElse(
                tagValue -> pointers.putIfAbsent(tagValue, pointer),
                () -> reportInvalidValue(tag.trim(), definition.get(), pointer));
      }

      private void reportInvalidValue(String tag, TagDefinition definition, String pointer) {
        report(
            pointer,
            "Contract tag [%s] is not a valid value of tag [%s]: %s",
            tag,
            definition.name(),
            definition.isBoolean()
                ? "it has no value"
                : "allowed values are " + definition.values());
      }

      /** The declared tags, with the highest value of a tag that is declared more than once. */
      TableTags toTags() {
        return TableTags.of(pointers.keySet());
      }

      String pointerOf(TagValue tag) {
        return pointers.get(tag);
      }
    }

    private void report(String pointer, String message, Object... args) {
      differences.add(new Difference(pointer, message.formatted(args)));
    }
  }

  /**
   * The properties of a contract matched with the columns or fields of the table by name. A name
   * matches exactly, or ignoring case if that is unambiguous.
   */
  private static final class Matching {

    private final List<OdcsProperty> columns;

    /** The property of each column or field that has one */
    private final Map<OdcsProperty, Located> matched = new HashMap<>();

    /** The columns or fields without a property */
    private final List<OdcsProperty> missing = new ArrayList<>();

    /** The properties without a column or field */
    private final List<Located> unmatched;

    Matching(List<Located> properties, List<OdcsProperty> columns) {
      this.columns = columns;
      unmatched = new ArrayList<>(properties);
      // Exact names first, so that two columns whose names differ only in case can both match
      var withoutExactName = new ArrayList<OdcsProperty>();
      for (var column : columns) {
        unmatched.stream()
            .filter(property -> column.name().equals(property.name()))
            .findFirst()
            .ifPresentOrElse(
                property -> match(column, property), () -> withoutExactName.add(column));
      }
      for (var column : withoutExactName) {
        var candidates =
            unmatched.stream()
                .filter(property -> column.name().equalsIgnoreCase(property.name()))
                .toList();
        if (candidates.size() == 1) {
          match(column, candidates.get(0));
        } else if (candidates.isEmpty()) {
          missing.add(column);
        }
        // Several candidates stay unmatched and are reported as naming the same column
      }
    }

    /** Calls the action for each column or field that has a property, in the order of the table. */
    void forEachMatch(BiConsumer<OdcsProperty, Located> action) {
      columns.stream()
          .filter(matched::containsKey)
          .forEach(column -> action.accept(column, matched.get(column)));
    }

    private void match(OdcsProperty column, Located property) {
      matched.put(column, property);
      unmatched.remove(property);
    }
  }

  private static String describeType(OdcsProperty property) {
    return StringUtils.defaultString(property.physicalType())
        + (property.mustBeSet() ? " NOT NULL" : "");
  }

  private static String names(List<OdcsProperty> properties) {
    return properties.stream().map(OdcsProperty::name).collect(Collectors.joining(", "));
  }
}
