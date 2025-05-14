package com.datasqrl.util;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.table.catalog.ObjectIdentifier;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

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

  public static ObjectIdentifier toIdentifier(Name name) {
    return toIdentifier(name.getDisplay());
  }


  public static ObjectIdentifier toIdentifier(String name) {
    return ObjectIdentifier.of("default_catalog", "default_database", name);
  }
}