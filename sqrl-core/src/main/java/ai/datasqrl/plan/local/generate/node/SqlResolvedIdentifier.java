package ai.datasqrl.plan.local.generate.node;

import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqlResolvedIdentifier extends SqlIdentifier {

  private final ResolvedNamePath namePath;

  public SqlResolvedIdentifier(ResolvedNamePath namePath, SqlParserPos pos) {
    super(namePath.getPath().stream().map(e->e.getName().getCanonical()).collect(Collectors.joining(".")), pos);
    this.namePath = namePath;
  }
}