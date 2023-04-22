package com.datasqrl.config;

import com.datasqrl.io.tables.TableConfig;
import lombok.Value;

public interface SinkFactoryContext {

  public String getTableName();

  public TableConfig getTableConfig();

}
