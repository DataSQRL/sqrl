package ai.datasqrl.plan.local;

import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlIdentifier extends SqlIdentifier {

  private final ResolvedNamePath namePath;

  public SqrlIdentifier(ResolvedNamePath namePath, SqlParserPos pos) {
    super(namePath.getPath().stream().map(e->e.getName().getCanonical()).collect(Collectors.joining(".")), pos);
    this.namePath = namePath;
  }
}