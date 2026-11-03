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
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.hint.DeclassifyHint;
import com.datasqrl.planner.hint.PlannerHints;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

/**
 * Computes how tags propagate from the input tables of a table definition and validates them.
 *
 * <ul>
 *   <li>Inherited tags: the union of the effective tags of all input tables, taking the maximum
 *       value per tag.
 *   <li>Effective tags: the inherited tags combined with the declared tags (maximum per tag), where
 *       each declassified tag takes its declared value or is removed if it is not declared.
 * </ul>
 *
 * Validation checks that declared tags are valid, that declassified tags are defined, that public
 * tables declare the tags they inherit (unless declassified), and that tables carrying a tag have
 * the columns the tag requires.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TagAnalyzer {

  /**
   * @param inherited the tags inherited from the input tables
   * @param effective the effective tags of the table
   */
  public record TagResult(TableTags inherited, TableTags effective) {}

  /** The normalized tag names declassified by the {@code declassify} hints */
  public static Set<String> getDeclassified(PlannerHints hints) {
    return hints
        .getHints(DeclassifyHint.class)
        .flatMap(hint -> hint.getTagNames().stream())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * Computes the inherited and effective tags of a table.
   *
   * @param fromTables the input tables of the table definition
   * @param declared the declared tags of the table
   * @param declassified the declassified tag names
   * @return the inherited and effective tags
   */
  public static TagResult compute(
      List<? extends TableOrFunctionAnalysis> fromTables,
      TableTags declared,
      Set<String> declassified) {
    var inherited =
        fromTables.stream()
            .map(TableOrFunctionAnalysis::getTags)
            .map(TableTags::withoutInvalidTags)
            .reduce(TableTags.EMPTY, TableTags::merge);
    var validDeclared = declared.withoutInvalidTags();
    var effective = inherited.merge(validDeclared).declassify(declassified, validDeclared);
    return new TagResult(inherited, effective);
  }

  /**
   * Validates the tags of a table and records all violations as errors without aborting, so all
   * violations are reported in one compilation.
   *
   * @param tableName the name of the table for error messages
   * @param definitions the tag definitions
   * @param result the computed tags of the table
   * @param declared the declared tags of the table, including invalid tags
   * @param declassified the declassified tag names
   * @param fromTables the input tables, to name where an inherited tag comes from
   * @param rowType the row type of the table, to check required columns; null to skip the check
   * @param validateDeclared whether to check that inherited tags are declared (public tables)
   * @param errors the error collector
   */
  public static void validate(
      String tableName,
      TagDefinitions definitions,
      TagResult result,
      TableTags declared,
      Set<String> declassified,
      List<? extends TableOrFunctionAnalysis> fromTables,
      RelDataType rowType,
      boolean validateDeclared,
      ErrorCollector errors) {
    // Declared tags must be defined and have valid values
    for (var invalid : declared.getInvalidTags()) {
      errors.error("Invalid tag on table [%s]: %s", tableName, invalid.message(definitions));
    }
    // Declassified tags must be defined and should be inherited
    for (var tag : declassified.stream().sorted().toList()) {
      if (definitions.get(tag).isEmpty()) {
        errors.error(
            "Table [%s] declassifies tag [%s] which is not defined in compiler.tags",
            tableName, tag);
      } else if (!result.inherited().contains(tag)) {
        errors.warn(
            "Table [%s] declassifies tag [%s] which it does not inherit from its input tables",
            tableName, tag);
      }
    }
    // Public tables must declare the tags they inherit, at the same or a higher value
    if (validateDeclared) {
      for (var tag : result.inherited().names().stream().sorted().toList()) {
        if (declassified.contains(tag)) {
          continue;
        }
        var def = definitions.getOrThrow(tag);
        var required = result.inherited().get(tag).getAsInt();
        var found = declared.get(tag);
        var sources = getSources(fromTables, tag, required);
        if (found.isEmpty()) {
          errors.error(
              "Table [%s] must declare tag [%s] inherited from [%s]: add it to the doc string or"
                  + " declassify it with /*+declassify(%s) */",
              tableName, render(def, required), sources, tag);
        } else if (found.getAsInt() < required) {
          errors.error(
              "Table [%s] declares tag [%s] which is lower than [%s] inherited from [%s]: raise it"
                  + " or declassify it with /*+declassify(%s) */",
              tableName, render(def, found.getAsInt()), render(def, required), sources, tag);
        }
      }
    }
    // Tables carrying a tag must have the columns the tag requires
    if (rowType != null) {
      for (var tag : result.effective().names().stream().sorted().toList()) {
        var def = definitions.getOrThrow(tag);
        for (var column : def.requiredColumns()) {
          var field = rowType.getField(column.name(), false, false);
          if (field == null) {
            errors.error(
                "Table [%s] has tag [%s] which requires column [%s] of type [%s], but the column is"
                    + " missing",
                tableName,
                render(def, result.effective().get(tag).getAsInt()),
                column.name(),
                column.typeDefinition());
          } else if (!TypeCompatibility.isCallSiteCompatible(field.getType(), column.type())) {
            errors.error(
                "Table [%s] has tag [%s] which requires column [%s] of type [%s], but the column"
                    + " has type [%s]",
                tableName,
                render(def, result.effective().get(tag).getAsInt()),
                column.name(),
                column.typeDefinition(),
                typeName(field.getType()));
          }
        }
      }
    }
  }

  private static String getSources(
      List<? extends TableOrFunctionAnalysis> fromTables, String tag, int offset) {
    return fromTables.stream()
        .filter(tbl -> tbl.getTags().get(tag).orElse(-1) == offset)
        .map(tbl -> tbl.getIdentifier().objectIdentifier().getObjectName())
        .distinct()
        .sorted()
        .collect(Collectors.joining(", "));
  }

  /** Renders a tag as written in a doc string, e.g. {@code @pii=raw} or {@code @gdpr} */
  static String render(TagDefinition def, int offset) {
    return def.isBoolean() ? "@" + def.name() : "@" + def.name() + "=" + def.valueAt(offset);
  }

  private static String typeName(RelDataType type) {
    try {
      return FlinkTypeFactory.toLogicalType(type).asSummaryString();
    } catch (Exception e) {
      return type.getFullTypeString();
    }
  }
}
