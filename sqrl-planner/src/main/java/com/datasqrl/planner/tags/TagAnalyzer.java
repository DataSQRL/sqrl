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
package com.datasqrl.planner.tags;

import com.datasqrl.calcite.type.TypeCompatibility;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.hint.DeclassifyHint;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

/**
 * Propagates tags from the input tables of a table to the table and validates them:
 *
 * <ul>
 *   <li>The <b>inherited</b> tags are the tags of all input tables, with the highest value per tag.
 *   <li>The <b>declared</b> tags are the tags in the table's documentation and, for a created
 *       table, in its column comments.
 *   <li>The <b>effective</b> tags are the inherited tags that are not declassified with the {@code
 *       declassify} hint, combined with the declared tags, with the highest value per tag.
 *       Downstream tables inherit the effective tags, and the deployment assets list them.
 * </ul>
 *
 * <p>All violations are recorded without aborting, so that a compilation reports all of them.
 */
@RequiredArgsConstructor
public class TagAnalyzer {

  private final TagDefinitions definitions;

  /**
   * Reports each tag in a doc string or column comment that is undefined or has an invalid value.
   * Texts are validated once where they are written, and {@link #analyze} ignores invalid tags, so
   * documentation that a table takes from another table doesn't report them again.
   */
  public void validateTags(String text, ErrorCollector errors) {
    var collector = errors.abortOnFatal(false);
    definitions.parse(text, message -> collector.fatal("%s", message));
  }

  /**
   * Computes the effective tags of a table, function, or relationship and validates that it
   * declares the tags it inherits and has the columns its tags require.
   *
   * @param table the analyzed table
   * @param columnComments the column comments of a created table, which declare tags too
   * @param mustDeclare whether the table must declare the tags it inherits
   * @return the effective tags of the table
   */
  public TableTags analyze(
      TableAnalysis table,
      List<String> columnComments,
      boolean mustDeclare,
      ErrorCollector errors) {

    if (!definitions.isEnabled()) {
      return TableTags.EMPTY;
    }

    errors = errors.abortOnFatal(false);
    var declared = declaredTags(table.getDocumentation(), columnComments);
    var inherited = inheritedTags(table.getFromTables());
    var required = inherited.without(declassifiedTags(table, inherited, errors));

    if (mustDeclare) {
      checkDeclared(table.getName(), required, declared, table.getFromTables(), true, errors);
    }

    var effective = required.merge(declared);
    checkRequiredColumns(table, effective, errors);

    return effective;
  }

  /**
   * Validates that a table declares the tags of the data that {@code INSERT INTO} or {@code EXPORT}
   * writes into it, since the tables reading from it only inherit its declared tags.
   *
   * @param target the table that is written into
   * @param inputs the tables the written data is computed from
   */
  public void checkWrite(
      TableAnalysis target, List<? extends TableOrFunctionAnalysis> inputs, ErrorCollector errors) {

    if (definitions.isEnabled()) {
      checkDeclared(
          target.getName(),
          inheritedTags(inputs),
          target.getTags(),
          inputs,
          false,
          errors.abortOnFatal(false));
    }
  }

  private TableTags declaredTags(Documentation documentation, List<String> columnComments) {
    return Stream.of(
            Stream.ofNullable(documentation.docString()),
            documentation.columnDocs().values().stream(),
            documentation.argumentDocs().values().stream(),
            columnComments.stream())
        .flatMap(Function.identity())
        .map(text -> definitions.parse(text, invalidTag -> {}))
        .reduce(TableTags.EMPTY, TableTags::merge);
  }

  private static TableTags inheritedTags(List<? extends TableOrFunctionAnalysis> inputs) {
    return inputs.stream()
        .map(TableOrFunctionAnalysis::getTags)
        .reduce(TableTags.EMPTY, TableTags::merge);
  }

  /** The tags of the table's declassify hints, which must be defined and should be inherited. */
  private Set<String> declassifiedTags(
      TableAnalysis table, TableTags inherited, ErrorCollector errors) {

    var declassified =
        table
            .getHints()
            .getHints(DeclassifyHint.class)
            .flatMap(hint -> hint.getTagNames().stream())
            .collect(Collectors.toCollection(TreeSet::new));

    for (var tag : declassified) {
      if (definitions.get(tag).isEmpty()) {
        errors.fatal(
            "Table [%s] declassifies tag [%s] which is not defined in compiler.tags",
            table.getName(), tag);
      } else if (inherited.get(tag).isEmpty()) {
        errors.warn(
            "Table [%s] declassifies tag [%s] which it does not inherit from its input tables",
            table.getName(), tag);
      }
    }

    return declassified;
  }

  private static void checkDeclared(
      String table,
      TableTags required,
      TableTags declared,
      List<? extends TableOrFunctionAnalysis> inputs,
      boolean canDeclassify,
      ErrorCollector errors) {

    for (var tag : required.values()) {
      var found = declared.get(tag.name());
      var fix =
          canDeclassify
              ? "declare it in the doc string or declassify it with /*+declassify(%s) */"
                  .formatted(tag.name())
              : "declare it in the doc string of the table";

      if (found.isEmpty()) {
        errors.fatal(
            "Table [%s] must declare tag [%s] inherited from [%s]: %s",
            table, tag, sourcesOf(tag, inputs), fix);

      } else if (found.get().isLowerThan(tag)) {
        errors.fatal(
            "Table [%s] declares tag [%s] which is lower than [%s] inherited from [%s]: %s",
            table, found.get(), tag, sourcesOf(tag, inputs), fix);
      }
    }
  }

  /** The names of the input tables that carry the tag with the given value. */
  private static String sourcesOf(TagValue tag, List<? extends TableOrFunctionAnalysis> inputs) {
    return inputs.stream()
        .filter(input -> input.getTags().get(tag.name()).filter(tag::equals).isPresent())
        .map(input -> input.getIdentifier().objectIdentifier().getObjectName())
        .distinct()
        .sorted()
        .collect(Collectors.joining(", "));
  }

  private static void checkRequiredColumns(
      TableAnalysis table, TableTags tags, ErrorCollector errors) {

    for (var tag : tags.values()) {
      for (var column : tag.definition().requiredColumns()) {
        var field = table.getField(column.name());

        if (field == null) {
          errors.fatal(
              "Table [%s] has tag [%s] which requires column [%s] of type [%s], but the column is"
                  + " missing",
              table.getName(), tag, column.name(), column.typeName());
        } else if (!TypeCompatibility.isCallSiteCompatible(field.getType(), column.type())) {
          errors.fatal(
              "Table [%s] has tag [%s] which requires column [%s] of type [%s], but the column has"
                  + " type [%s]",
              table.getName(),
              tag,
              column.name(),
              column.typeName(),
              FlinkTypeFactory.toLogicalType(field.getType()).asSummaryString());
        }
      }
    }
  }
}
