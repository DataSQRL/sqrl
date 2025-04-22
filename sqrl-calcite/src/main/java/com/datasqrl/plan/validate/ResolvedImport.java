package com.datasqrl.plan.validate;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.config.TableConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ResolvedImport {

  String name;
  private final TableConfig tableConfig;
  private final RelDataType relDataType;
}
