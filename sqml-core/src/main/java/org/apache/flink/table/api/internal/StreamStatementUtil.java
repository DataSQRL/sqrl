package org.apache.flink.table.api.internal;

import java.util.List;
import org.apache.flink.table.api.bridge.java.internal.StreamStatementSetImpl;
import org.apache.flink.table.operations.ModifyOperation;

public class StreamStatementUtil {

  public static List<ModifyOperation> getOperations(StreamStatementSetImpl streamStatementSet) {
    return streamStatementSet.operations;
  }
}
