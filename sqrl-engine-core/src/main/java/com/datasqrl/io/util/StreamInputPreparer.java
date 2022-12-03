package com.datasqrl.io.util;

import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.physical.stream.StreamEngine;
import com.datasqrl.physical.stream.StreamHolder;

public interface StreamInputPreparer {

    boolean isRawInput(TableInput table);

    StreamHolder<SourceRecord.Raw> getRawInput(TableInput table, StreamEngine.Builder builder);

//    void importTable(ImportManager.SourceTableImport tableImport, StreamEngine.Builder builder);

}
