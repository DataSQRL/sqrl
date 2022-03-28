package ai.dataeng.sqml.parser;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

@Value
public class ParseResult {
  SqlNode sqlNode;
  List<String> primaryKeyHint;
  SqlValidator validator;
  Optional<Table> destinationTable;
  Map<Column, String> aliases;
}
