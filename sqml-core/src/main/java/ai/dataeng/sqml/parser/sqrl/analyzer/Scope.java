package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.StatementAnalyzer.AnalyzerField;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Builder
@Getter
@Setter
public class Scope {
  private Optional<Table> contextTable;
  private Node node;
  private List<AnalyzerField> fields;

  public List<AnalyzerField> resolveFieldsWithPrefix(Optional<Name> starPrefix) {
    return null;
  }
}
