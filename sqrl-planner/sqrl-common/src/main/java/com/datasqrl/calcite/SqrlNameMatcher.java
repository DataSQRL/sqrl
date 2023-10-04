package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

/**
 * Allows for choosing the most recent table function.
 */
@AllArgsConstructor
public class SqrlNameMatcher {
  NameCanonicalizer canonicalizer;

  final SqlNameMatcher delegate = SqlNameMatchers.withCaseSensitive(false);

  public static String getLatestVersion(NameCanonicalizer canonicalizer, Collection<String> functionNames, String prefix) {
    //todo: use name comparator
    Set<String> functionNamesCanon = functionNames.stream()
        .map(f-> canonicalizer.name(f).getCanonical())
        .collect(Collectors.toSet());
    String prefixCanon = canonicalizer.name(prefix).getCanonical();

    Pattern pattern = Pattern.compile("^"+Pattern.quote(prefixCanon) + "\\$(\\d+)");
    int maxVersion = -1;

    String name = null;
    for (String function : functionNamesCanon) {
      Matcher matcher = pattern.matcher(function);
      if (matcher.find()) {
        int version = Integer.parseInt(matcher.group(1));
        maxVersion = Math.max(maxVersion, version);
        if (maxVersion == version) {
          name = matcher.group();
        }
      }
    }

    return maxVersion != -1 ? name : null;
  }
}
