package org.apache.calcite.sql;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class SqrlCompoundIdentifier extends SqlNodeList {

  private final List<SqlNode> items;

  public SqrlCompoundIdentifier(SqlParserPos pos, List<SqlNode> items) {
    super(List.of(), pos);
    this.items = items;
  }
}
