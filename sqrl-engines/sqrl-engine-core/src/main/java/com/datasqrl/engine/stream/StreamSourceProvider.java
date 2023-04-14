package com.datasqrl.engine.stream;

import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.util.TimeAnnotatedRecord;

public interface StreamSourceProvider {

  StreamHolder<TimeAnnotatedRecord<String>> fromTextSource(TableInput table);

}
