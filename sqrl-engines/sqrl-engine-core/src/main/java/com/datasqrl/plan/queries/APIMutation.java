package com.datasqrl.plan.queries;

import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class APIMutation implements APIConnector {

  @EqualsAndHashCode.Include
  Name name;
  @EqualsAndHashCode.Include
  APISource source;
  RelDataType schema;

  @Override
  public String toString() {
    return NamePath.of(source.getName(),name).toString();
  }

  public NamedRelDataType getSchema() {
    return new NamedRelDataType(name, schema);
  }


}
