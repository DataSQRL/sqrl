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
package com.datasqrl.compile.check;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorLocation.FileRange;
import com.datasqrl.planner.parser.SQLStatement;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Util;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.hint.StateTtlHint;

/**
 * The hints of Flink in queries, e.g. {@code BROADCAST(c)} after {@code SELECT}, unlike the hints
 * of SQRL before statements. Flink rejects a join hint or {@code STATE_TTL} whose options name no
 * input of the query only when it optimizes the plan, e.g. when the job is deployed, so the checker
 * reports those options itself.
 */
public final class QueryHints {

  /** The hints after {@code SELECT}: the join strategies, and the time-to-live of state */
  public static final List<String> QUERY_HINTS =
      Stream.concat(
              Arrays.stream(JoinStrategy.values()).map(JoinStrategy::getJoinHintName),
              Stream.of(StateTtlHint.STATE_TTL.getHintName()))
          .toList();

  /** The hints after a table, which set options of its connector */
  public static final List<String> TABLE_HINTS = List.of(FlinkHints.HINT_NAME_OPTIONS);

  private QueryHints() {}

  /**
   * An option of a hint that names no input of its query.
   *
   * @param position the position of the option in the query
   * @param message the error
   */
  public record Problem(SqlParserPos position, String message) {}

  /**
   * A problem in a build file, which the checker maps to its source file.
   *
   * @param buildFile the build file
   * @param range the range in the build file
   * @param message the error
   */
  record Located(Path buildFile, FileRange range, String message) {}

  /**
   * The problems of a query of a statement, at their positions in the build file of the statement.
   *
   * @param query the validated query
   * @param statement the statement, which maps positions in its query to positions in itself
   * @param file the build file of the statement
   * @param location the start of the statement in the file
   * @param lastLine the last line of the query that is in the statement
   */
  static List<Located> located(
      SqlNode query, SQLStatement statement, Path file, FileLocation location, int lastLine) {
    var located = new ArrayList<Located>();
    for (var problem : problems(query)) {
      var position = problem.position();
      if (position.getLineNum() > lastLine) {
        continue;
      }
      var start =
          location.add(
              statement.mapSqlLocation(
                  new FileLocation(position.getLineNum(), position.getColumnNum())));
      var end =
          location.add(
              statement.mapSqlLocation(
                  new FileLocation(position.getEndLineNum(), position.getEndColumnNum() + 1)));
      located.add(
          new Located(
              file,
              new FileRange(start.line(), start.offset(), end.line(), end.offset()),
              problem.message()));
    }
    return located;
  }

  /**
   * Whether a hint is a join hint whose options are inputs of its query, e.g. {@code BROADCAST}.
   */
  public static boolean isJoinHint(String hint) {
    var name = hint.toUpperCase(Locale.ROOT);
    return JoinStrategy.isJoinStrategy(name) && !JoinStrategy.isLookupHint(name);
  }

  /** Whether options of a hint name inputs: of a join hint, or the keys of {@code STATE_TTL}. */
  private static boolean namesInputs(String hint) {
    return isJoinHint(hint) || StateTtlHint.isStateTtlHint(hint.toUpperCase(Locale.ROOT));
  }

  /** The options of the hints in a validated query that name no table or alias of their query. */
  public static List<Problem> problems(SqlNode query) {
    var problems = new ArrayList<Problem>();
    query.accept(
        new SqlBasicVisitor<Void>() {
          @Override
          public Void visit(SqlCall call) {
            if (call instanceof SqlSelect select && select.hasHints()) {
              check(select, problems);
            }
            return super.visit(call);
          }
        });
    return problems;
  }

  private static void check(SqlSelect select, List<Problem> problems) {
    var inputs = new Inputs(new HashSet<>(), new LinkedHashSet<>());
    inputs.add(select.getFrom());
    if (inputs.names().isEmpty()) {
      return; // e.g. a subquery without an alias, whose names Flink does not know either
    }
    var names =
        inputs.names().stream().map(name -> "`" + name + "`").collect(Collectors.joining(", "));
    for (var node : select.getHints()) {
      if (!(node instanceof SqlHint hint) || !namesInputs(hint.getName())) {
        continue;
      }
      for (var option : namedInputs(hint)) {
        var name = optionText(option);
        if (name != null && !inputs.accepted().contains(name.toLowerCase(Locale.ROOT))) {
          problems.add(
              new Problem(
                  option.getParserPosition(),
                  ("Hint %s names `%s`, which is not a table or alias of this query, so Flink"
                          + " rejects it. The query has: %s.")
                      .formatted(hint.getName().toUpperCase(Locale.ROOT), name, names)));
        }
      }
    }
  }

  /** The options that name inputs: all of a join hint, the keys of {@code STATE_TTL}. */
  private static List<SqlNode> namedInputs(SqlHint hint) {
    var options = (SqlNodeList) hint.getOperandList().get(1);
    if (hint.getOptionFormat() != HintOptionFormat.KV_LIST) {
      return options.getList();
    }
    var keys = new ArrayList<SqlNode>();
    for (var i = 0; i < options.size(); i += 2) {
      keys.add(options.get(i));
    }
    return keys;
  }

  private static String optionText(SqlNode option) {
    if (option instanceof SqlIdentifier identifier) {
      return Util.last(identifier.names);
    } else if (option instanceof SqlLiteral literal) {
      return literal.getValueAs(String.class);
    }
    return null;
  }

  /**
   * The inputs of a query.
   *
   * @param accepted the names that options may use in lower case: the aliases and table names
   * @param names the names that Flink gives the inputs: the alias of a table, or else its name
   */
  private record Inputs(Set<String> accepted, Set<String> names) {

    void add(SqlNode from) {
      if (from instanceof SqlJoin join) {
        add(join.getLeft());
        add(join.getRight());
      } else if (from instanceof SqlSnapshot snapshot) {
        add(snapshot.getTableRef());
      } else if (from instanceof SqlCall call
          && call.getKind() == SqlKind.AS
          && call.operand(1) instanceof SqlIdentifier alias) {
        var input =
            call.operand(0) instanceof SqlSnapshot snapshot
                ? snapshot.getTableRef()
                : call.operand(0);
        if (input instanceof SqlIdentifier table) {
          accepted.add(Util.last(table.names).toLowerCase(Locale.ROOT));
        }
        add(Util.last(alias.names));
      } else if (from instanceof SqlIdentifier table) {
        add(Util.last(table.names));
      }
    }

    private void add(String name) {
      accepted.add(name.toLowerCase(Locale.ROOT));
      names.add(name);
    }
  }
}
