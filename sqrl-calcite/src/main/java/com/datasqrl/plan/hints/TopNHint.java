/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public class TopNHint implements SqrlHint {

  public enum Type {
    DISTINCT_ON,
    SELECT_DISTINCT,
    TOP_N;

    public boolean matches(String name) {
      return name.equalsIgnoreCase(toString());
    }
  }

  final Type type;
  final List<Integer> partition;

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .hintOptions(partition.stream().map(String::valueOf).collect(Collectors.toList())).build();
  }

  public static SqlHint createSqlHint(Type type, SqlNodeList columns, SqlParserPos pos) {
    return new SqlHint(pos, new SqlIdentifier(type.toString(), pos), columns,
        SqlHint.HintOptionFormat.ID_LIST);
  }

  @Override
  public String getHintName() {
    return type.name();
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<TopNHint> {

    @Override
    public boolean validName(String name) {
      return Arrays.stream(Type.values()).anyMatch(v -> v.matches(name));
    }

    @Override
    public TopNHint fromHint(RelHint hint) {
      Type type = Type.valueOf(hint.hintName);
      List<Integer> partition = hint.listOptions.stream().map(Integer::parseInt)
          .collect(Collectors.toList());
      return new TopNHint(type, partition);
    }
  }

}
