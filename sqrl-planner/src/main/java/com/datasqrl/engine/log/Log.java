package com.datasqrl.engine.log;

import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import lombok.Value;

public interface Log {

  TableSource getSource();

  TableSink getSink();

  @Value
  class Impl {

    TableSource source;
    TableSink sink;

  }

}
