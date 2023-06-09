package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.UniversalTable;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class APIMutation implements APIConnector {

  @EqualsAndHashCode.Include
  Name name;
  @EqualsAndHashCode.Include
  APISource source;
  UniversalTable schema;

  @Override
  public String toString() {
    return NamePath.of(source.getName(),name).toString();
  }

}
