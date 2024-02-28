package com.datasqrl.io.tables;

import com.datasqrl.io.formats.FormatFactory;

public interface TableSink extends ExternalTable {
  FormatFactory.Writer getWriter();

}
