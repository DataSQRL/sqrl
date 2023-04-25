package com.datasqrl.config;

import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.tables.TableConfig;
import lombok.Value;

@Value
public class FlinkSinkFactoryContext implements SinkFactoryContext {

  String tableName;
  TableConfig tableConfig;
  FormatFactory formatFactory;

}
