package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.util.FileUtil;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class APISourceImpl implements APISource {

  @Include
  Name name;
  String schemaDefinition;

  public static APISource of(String schemaDefinition) {
    return new APISourceImpl(Name.system("schema"),schemaDefinition
        .replaceAll("\t", "  "));
  }

  @Override
  public String toString() {
    return name.toString();
  }

  public static APISource of(String filename, NameCanonicalizer canonicalizer, ResourceResolver resolver) {
    return new APISourceImpl(
        canonicalizer.name(FileUtil.separateExtension(filename).getKey()),
        FileUtil.readFile(resolver.resolveFile(NamePath.of(filename)).get())
    );
  }

  @Override
  public APISource clone(String schema) {
    return new APISourceImpl(name, schema);
  }
}
