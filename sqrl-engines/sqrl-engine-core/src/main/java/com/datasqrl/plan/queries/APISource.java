package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.util.FileUtil;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Value;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class APISource {

  @Include
  Name name;
  String path;
  String schemaDefinition;

  @Override
  public String toString() {
    return name.toString();
  }

  public static APISource of(String filename, NameCanonicalizer canonicalizer, ResourceResolver resolver) {
    return new APISource(
        canonicalizer.name(FileUtil.separateExtension(filename).getKey()),
        filename,
        FileUtil.readFile(resolver.resolveFile(NamePath.of(filename)).get())
    );
  }

}
