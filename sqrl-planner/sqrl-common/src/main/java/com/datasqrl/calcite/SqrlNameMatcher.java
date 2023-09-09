package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

@AllArgsConstructor
public class SqrlNameMatcher implements SqlNameMatcher {
  NameCanonicalizer canonicalizer;
  final SqlNameMatcher delegate = SqlNameMatchers.withCaseSensitive(false);
  @Override
  public boolean isCaseSensitive() {
    return false;
  }

  @Override
  public boolean matches(String s, String s1) {
    return delegate.matches(s, s1);
  }

  @Override
  public <K extends List<String>, V> V get(Map<K, V> map, List<String> list, List<String> list1) {


    return delegate.get(map, list, list1);
  }

  @Override
  public String bestString() {
    return delegate.bestString();
  }

  @Override
  public RelDataTypeField field(RelDataType relDataType, String s) {
    RelDataTypeField f = relDataType.getField(s, false, false);
    if (f != null) {
      return f;
    }

    String name = SqrlRelBuilder.getLatestVersion(relDataType.getFieldNames(), s);
    if (name == null) {
      return null;
    }
    return relDataType.getField(name, false, false);
  }

  @Override
  public int indexOf(Iterable<String> names, String name) {
    List<String> n = new ArrayList<>();
    names.iterator().forEachRemaining(n::add);

    if (n.contains(name)) {
      return n.indexOf(name);
    }

    String versioned = SqrlRelBuilder.getLatestVersion(n, name);

    return n.indexOf(versioned);
  }

  @Override
  public int frequency(Iterable<String> iterable, String s) {
    return delegate.frequency(iterable, s);
  }

  @Override
  public Set<String> createSet() {
    return delegate.createSet();
  }
}
