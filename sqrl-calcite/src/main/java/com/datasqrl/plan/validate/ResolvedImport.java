package com.datasqrl.plan.validate;

import com.datasqrl.config.TableConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

@Getter
@AllArgsConstructor
public class ResolvedImport {

  String name;
  private final TableConfig tableConfig;
  private final RelDataType relDataType;
}
