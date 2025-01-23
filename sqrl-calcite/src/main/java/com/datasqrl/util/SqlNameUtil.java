package com.datasqrl.util;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.google.inject.Inject;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.flink.table.catalog.ObjectIdentifier;

@AllArgsConstructor(onConstructor_=@Inject)
public class SqlNameUtil {

  private final NameCanonicalizer canonicalizer;

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

  public static ObjectIdentifier toIdentifier(NamePath path) {
    return ObjectIdentifier.of("default_catalog", "default_database", path.getLast().getDisplay());
  }
}