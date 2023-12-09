package com.datasqrl.util;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import java.util.Arrays;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlExpressionQuery;

public class SqlNameUtil {

  private final NameCanonicalizer canonicalizer;

  public SqlNameUtil(NameCanonicalizer canonicalizer) {
    this.canonicalizer = canonicalizer;
  }

  public NamePath toNamePath(List<String> names) {
    return NamePath.of(names.stream()
        .map(this::toName)
        .collect(Collectors.toList()));
  }

  public Name toName(String name) {
    return name.equals("")
        ? ReservedName.ALL
        : canonicalizer.name(name);
  }

  public static String toSafeName(String name) {
    return name.replaceAll("[^_0-9A-Za-z]+", "_");
  }

  public static List<String> toStringList(NamePath path) {
    return Arrays.stream(path.getNames())
        .map(e->e.getDisplay())
        .collect(Collectors.toList());
  }


  public NamePath getParentPath(SqrlAssignment statement) {
    NamePath path = toNamePath(statement.getIdentifier().names);
    if (statement instanceof SqrlExpressionQuery) {
      if (statement.getIdentifier().names.size() > 2) {
        return path.popLast().popLast();
      } else {
        return path.popLast();
      }
    } else {
      return path.popLast();
    }
  }
}