/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.util;

import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.StreamSourceProvider;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.tables.TableInput;

public interface StreamInputPreparer {

  boolean isRawInput(TableInput table);

  StreamHolder<SourceRecord.Raw> getRawInput(TableInput table, StreamSourceProvider builder,
      ErrorLocation errorLocation);

}
