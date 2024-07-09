package com.datasqrl.plan.validate;

import com.datasqrl.config.TableConfig;
import lombok.Getter;

@Getter
public class ResolvedImport {

  String name;
  private final TableConfig tableConfig;

  public ResolvedImport(String name, TableConfig tableConfig) {
    this.name = name;
    this.tableConfig = tableConfig;
  }
}
