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

import com.datasqrl.planner.contract.ContractSubject;
import com.datasqrl.planner.contract.ContractSubject.ColumnKind;
import com.datasqrl.planner.contract.odcs.model.OdcsMap;
import com.datasqrl.planner.contract.odcs.model.OdcsProperty;
import com.datasqrl.planner.contract.odcs.model.OdcsSchemaObject;
import com.datasqrl.planner.tags.TableTags;
import com.datasqrl.planner.tags.TagDefinition;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.planner.tags.TagValue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 *       order. The column that the watermark is declared on is exempt in two cases, since
 *       environments differ in how they provide it: it may be missing from the contract if the
 *       table adds it itself to the columns that it takes over with {@code LIKE}, and the contract
 *       may list it when the table reads it from metadata or computes it.
 *   <li>{@code logicalType} and {@code required} must be equal, also in nested properties, array
 *       items, and map keys and values.
 *   <li>A primary key or partition key that the contract declares must be the key of the table.
 *   <li>The tags that are defined in {@code compiler.tags} are compared for each column and for the
 *       table. A property must list the tags of its column, where its {@code classification} is the
 *       tag of that name. And the tags of the schema object, together with the tags of its
 *       properties, must be the tags of the table, so the classification of the table is a {@code
 *       classification=<value>} tag of the schema object.
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
   * @param generated the schema object generated for the table
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
      comparison.tags.table(provided.tags(), pointer);
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

    private final TagComparison tags = new TagComparison();

    Comparison(ContractSubject subject, List<Located> properties) {
      this.subject = subject;
      this.properties = properties;
    }

    void columns(List<OdcsProperty> columns, String pointer) {
      var matching = new Matching(properties, columns);
      matching.forEachMatch(
          (column, property) -> {
            element(property.property(), property.pointer(), column, column.name(), true);
            if (tagDefinitions.isEnabled()) {
              tags.column(property, column);
            }
          });

      for (var column : matching.missing) {
        if (!subject.mayOmitColumn(column.name())) {
          report(
              pointer,
              "Column [%s] (%s) of table [%s] is missing from the contract%s",
              column.name(),
              describeType(column),
              subject.name(),
              subject.isWatermarkColumn(column.name())
                  ? ". It has the watermark, and a contract may only leave out a watermark column"
                      + " that the table adds itself to the columns it takes over with LIKE"
                  : "");
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
      } else if (tagDefinitions.isEnabled()) {
        tags.withoutColumn(property);
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
      var contractColumns = contractKey.stream().map(OdcsCompatibilityChecker::key).toList();
      var tableColumns = tableKey.stream().map(OdcsCompatibilityChecker::key).toList();

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

    /**
     * Compares the tags of the contract that are defined in {@code compiler.tags} with the tags of
     * the table: for each column, and for the table as a whole. Other tags of the contract are
     * labels of the catalog.
     */
    private class TagComparison {

      /**
       * The tags of the columns that the contract has a property for: a tag that the property
       * lists, or one that is reported as missing from it
       */
      private final Set<TagValue> ofColumns = new HashSet<>();

      /**
       * The tags that the contract declares for the table as a whole: on the schema object, and on
       * a property whose column has no tags to compare them with
       */
      private final DeclaredTags ofTable = new DeclaredTags();

      /**
       * A property must list the tags of its column. Its classification is the tag of that name.
       *
       * @param column the property generated for the column, with the tags and the classification
       *     that the column declares
       */
      void column(Located property, OdcsProperty column) {
        var declared = new DeclaredTags();
        declared.addProperty(property.property(), property.pointer());
        var contractTags = declared.toTags();
        var columnTags = new DeclaredTags();
        columnTags.addProperty(column, "");
        var tagsOfColumn = columnTags.toTags();

        for (var tag : contractTags.values()) {
          var ofColumn = tagsOfColumn.get(tag.name());
          if (ofColumn.filter(tag::equals).isPresent()) {
            ofColumns.add(tag);
          } else if (subject.tags().get(tag.name()).isEmpty()) {
            // The table does not have the tag at all, which is reported once for the table
            ofTable.add(tag, declared.pointerOf(tag));
          } else {
            // The difference is reported here, and not again for the table
            subject.tags().get(tag.name()).ifPresent(ofColumns::add);
            ofColumn.ifPresent(ofColumns::add);
            reportOtherTag(tag, ofColumn, declared.pointerOf(tag), property, column);
          }
        }
        for (var tag : tagsOfColumn.values()) {
          if (contractTags.get(tag.name()).isEmpty()) {
            ofColumns.add(tag);
            var classification = isClassification(tag);
            report(
                property.pointer()
                    + "/"
                    + (classification
                        ? OdcsConstants.CLASSIFICATION_FIELD
                        : OdcsConstants.TAGS_FIELD),
                "%s [%s] of column [%s] of table [%s] is missing from property [%s]",
                classification ? "classification" : "Tag",
                classification ? tag.value() : OdcsTags.write(tag),
                column.name(),
                subject.name(),
                property.name());
          }
        }
      }

      /** A property that the table has no physical column for declares its tags for the table. */
      void withoutColumn(Located property) {
        ofTable.addProperty(property.property(), property.pointer());
      }

      /**
       * The tags of the schema object, together with the tags of its properties, must be the tags
       * of the table.
       */
      void table(List<String> schemaObjectTags, String pointer) {
        var tagsPointer = pointer + "/" + OdcsConstants.TAGS_FIELD;
        ofTable.addAll(schemaObjectTags, tagsPointer);
        var declared = ofTable.toTags();

        for (var tag : declared.values()) {
          var tableTag = subject.tags().get(tag.name());
          if (tableTag.isEmpty()) {
            report(
                ofTable.pointerOf(tag),
                "The contract declares tag [%s] that table [%s] does not have",
                OdcsTags.write(tag),
                subject.name());
          } else if (!tableTag.get().equals(tag)) {
            report(
                ofTable.pointerOf(tag),
                "The contract declares tag [%s] but table [%s] has [%s]",
                OdcsTags.write(tag),
                subject.name(),
                OdcsTags.write(tableTag.get()));
          }
        }
        for (var tag : subject.tags().values()) {
          if (declared.get(tag.name()).isEmpty() && !ofColumns.contains(tag)) {
            report(
                tagsPointer,
                "Tag [%s] of table [%s] is missing from the contract",
                OdcsTags.write(tag),
                subject.name());
          }
        }
      }

      private void reportOtherTag(
          TagValue tag,
          Optional<TagValue> ofColumn,
          String pointer,
          Located property,
          OdcsProperty column) {
        var classification = isClassification(tag);
        if (ofColumn.isPresent() && classification) {
          report(
              pointer,
              "classification is [%s] in the contract, but column [%s] of table [%s] has [%s]",
              tag.value(),
              column.name(),
              subject.name(),
              ofColumn.get().value());
        } else if (ofColumn.isPresent()) {
          report(
              pointer,
              "Property [%s] declares tag [%s], but column [%s] of table [%s] has [%s]",
              property.name(),
              OdcsTags.write(tag),
              column.name(),
              subject.name(),
              OdcsTags.write(ofColumn.get()));
        } else {
          // The table has the tag, and the column does not declare it
          var tableTag = subject.tags().get(tag.name()).filter(ofTable -> !ofTable.equals(tag));
          report(
              pointer,
              "Property [%s] declares %s, but column [%s] of table [%s] does not declare it%s."
                  + " Declare it in the comment of the column, or list %s on the schema object",
              property.name(),
              classification
                  ? "classification [%s]".formatted(tag.value())
                  : "tag [%s]".formatted(OdcsTags.write(tag)),
              column.name(),
              subject.name(),
              tableTag
                  .map(ofTable -> " and the table has [%s]".formatted(OdcsTags.write(ofTable)))
                  .orElse(""),
              classification ? "the classification of the table as a tag" : "the tag of the table");
        }
      }

      private static boolean isClassification(TagValue tag) {
        return tag.name().equalsIgnoreCase(OdcsConstants.CLASSIFICATION_TAG);
      }
    }

    /** Tags of a contract that are defined in {@code compiler.tags}, with where they are listed. */
    private class DeclaredTags {

      /** The pointer of the first declaration of each tag value */
      private final Map<TagValue, String> pointers = new LinkedHashMap<>();

      void addAll(List<String> tags, String pointer) {
        IntStream.range(0, tags.size()).forEach(i -> add(tags.get(i), pointer + "/" + i));
      }

      /** Adds the tags of a property, of the properties nested in it, and its classification. */
      void addProperty(OdcsProperty property, String pointer) {
        addAll(property.tags(), pointer + "/" + OdcsConstants.TAGS_FIELD);
        if (property.classification() != null) {
          add(
              new OdcsTags.ContractTag(OdcsConstants.CLASSIFICATION_TAG, property.classification()),
              pointer + "/" + OdcsConstants.CLASSIFICATION_FIELD);
        }
        var nested = property.properties();
        IntStream.range(0, nested.size())
            .forEach(
                i ->
                    addProperty(
                        nested.get(i), pointer + "/" + OdcsConstants.PROPERTIES_FIELD + "/" + i));
        if (property.items() != null) {
          addProperty(property.items(), pointer + "/items");
        }
        if (property.map() != null) {
          addProperty(property.map().key(), pointer + "/map/key");
          addProperty(property.map().value(), pointer + "/map/value");
        }
      }

      /**
       * @param tag a tag as a contract writes it: {@code name} or {@code name=value}
       */
      void add(String tag, String pointer) {
        add(OdcsTags.parse(tag), pointer);
      }

      void add(OdcsTags.ContractTag tag, String pointer) {
        var definition = tagDefinitions.get(tag.name());
        if (definition.isEmpty()) {
          return; // a label of the catalog
        }
        definition
            .get()
            .valueOf(tag.value())
            .ifPresentOrElse(
                tagValue -> add(tagValue, pointer),
                () -> reportInvalidValue(tag.toString(), definition.get(), pointer));
      }

      void add(TagValue tag, String pointer) {
        pointers.putIfAbsent(tag, pointer);
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

  /** Column names are compared ignoring case. */
  private static String key(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
