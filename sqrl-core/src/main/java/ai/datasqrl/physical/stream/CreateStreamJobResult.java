package ai.datasqrl.physical.stream;

import java.util.List;
import lombok.Value;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

@Value
public class CreateStreamJobResult {
  StreamStatementSet streamQueries;
  List<TableDescriptor> createdTables;
}
