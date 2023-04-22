package com.datasqrl.config;

import com.datasqrl.io.tables.TableConfig;
import lombok.Value;

public interface SourceFactoryContext {

  public TableConfig getTableConfig();

  @Value
  class Implementation implements SourceFactoryContext {
    TableConfig tableConfig;
  }

}
