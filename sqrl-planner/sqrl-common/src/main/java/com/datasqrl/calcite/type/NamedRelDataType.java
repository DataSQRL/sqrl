package com.datasqrl.calcite.type;

import com.datasqrl.canonicalizer.Name;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
public class NamedRelDataType {

  Name name;
  RelDataType type;


}
