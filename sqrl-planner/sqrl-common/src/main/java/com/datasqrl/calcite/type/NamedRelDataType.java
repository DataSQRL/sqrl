package com.datasqrl.calcite.type;

import com.datasqrl.canonicalizer.Name;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@AllArgsConstructor
@Getter
public class NamedRelDataType {

  Name name;
  RelDataType type;
}
