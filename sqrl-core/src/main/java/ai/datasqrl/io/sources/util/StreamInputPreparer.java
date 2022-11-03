package ai.datasqrl.io.sources.util;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.TableInput;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.StreamHolder;

public interface StreamInputPreparer {

    boolean isRawInput(TableInput table);

    StreamHolder<SourceRecord.Raw> getRawInput(TableInput table, StreamEngine.Builder builder);

//    void importTable(ImportManager.SourceTableImport tableImport, StreamEngine.Builder builder);

}
