package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class APIMutation implements APIConnector {

  @EqualsAndHashCode.Include
  Name name;
  @EqualsAndHashCode.Include
  APISource source;

  RelDataType schema;
  String timestampName;
  String pkName;

  @Override
  public String toString() {
    return NamePath.of(source.getName(), name).toString();
  }

  public RelDataTypeField getSchema() {
    return new RelDataTypeFieldImpl(name.getDisplay(), -1, schema);
  }
}
