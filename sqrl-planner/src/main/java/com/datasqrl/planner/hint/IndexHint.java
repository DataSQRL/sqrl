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
package com.datasqrl.planner.hint;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.plan.global.IndexType;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.RelFieldCollation.Direction;

/**
 * Explicitly assign an index to a table that's persisted to a database engine. Overwrites the
 * automatically determined index structures.
 */
@Getter
public class IndexHint extends ColumnNamesHint {

  public static final String HINT_NAME = "index";

  private final IndexType indexType;
  private final List<Direction> directions;

  protected IndexHint(
      ParsedObject<SqrlHint> source,
      IndexType indexType,
      List<String> columnsNames,
      List<Direction> directions) {
    super(source, Type.DAG, columnsNames);
    this.indexType = indexType;
    this.directions = directions;
  }

  @AutoService(Factory.class)
  public static class IndexHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      var arguments = source.get().options();
      if (arguments == null || arguments.isEmpty()) {
        return new IndexHint(source, null, List.of(), List.of()); // no hint
      }

      if (arguments.size() == 1) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "Index hint requires at least two arguments: the name of the index type and at least one column.");
      }
      var optIndex = IndexType.fromName(arguments.get(0));
      if (optIndex.isEmpty()) {
        throw new StatementParserException(
            ErrorLabel.GENERIC,
            source.getFileLocation(),
            "Unknown index type: %s",
            arguments.get(0));
      }
      var indexType = optIndex.get();
      var columns = parseColumns(source, indexType, arguments.subList(1, arguments.size()));

      return new IndexHint(source, indexType, columns.names(), columns.directions());
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }

    private static ParsedColumns parseColumns(
        ParsedObject<SqrlHint> source, IndexType indexType, List<String> arguments) {

      var columnNames = new ArrayList<String>();
      var directions = new ArrayList<Direction>();

      for (var argument : arguments) {
        var terms = argument.trim().split("\\s+");

        if (argument.isBlank() || terms.length > 2) {
          throw invalidColumnSpecification(source, argument);
        }

        columnNames.add(terms[0]);

        Direction direction = Direction.ASCENDING;
        if (terms.length > 1) {
          direction = parseDirection(source, argument, terms[1]);
        }

        if (direction.isDescending() && !indexType.supportsSortOrder()) {
          throw new StatementParserException(
              ErrorLabel.GENERIC,
              source.getFileLocation(),
              "Descending index columns are only supported for BTREE and PBTREE indexes.");
        }

        directions.add(direction);
      }

      return new ParsedColumns(columnNames, directions);
    }

    private static Direction parseDirection(
        ParsedObject<SqrlHint> source, String argument, String direction) {

      if ("asc".equalsIgnoreCase(direction)) {
        return Direction.ASCENDING;
      }

      if ("desc".equalsIgnoreCase(direction)) {
        return Direction.DESCENDING;
      }

      throw invalidColumnSpecification(source, argument);
    }

    private static StatementParserException invalidColumnSpecification(
        ParsedObject<SqrlHint> source, String argument) {
      return new StatementParserException(
          ErrorLabel.GENERIC,
          source.getFileLocation(),
          "Invalid index column specification: %s. Expected a column name optionally followed by ASC or DESC.",
          argument);
    }

    private record ParsedColumns(List<String> names, List<Direction> directions) {}
  }
}
