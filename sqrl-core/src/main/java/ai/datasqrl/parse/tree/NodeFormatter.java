package ai.datasqrl.parse.tree;

import java.util.stream.Collectors;

public class NodeFormatter extends AstVisitor<String, Object> {

  private static final NodeFormatter nodeFormatter = new NodeFormatter();

  public static String accept(Node node) {
    return node.accept(nodeFormatter, null);
  }

  @Override
  public String visitNode(Node node, Object context) {
    if (node != null) {
      throw new RuntimeException("Could not format sql node: " + node.getClass());
    }
    throw new RuntimeException("Could not format sql node");
  }

  @Override
  public String visitImportDefinition(ImportDefinition node, Object context) {
    return "IMPORT " + node.getNamePath() +
        node.getTimestamp().map(s -> " TIMESTAMP " + s.accept(this, context))
            .orElse("") + ";";
  }

  @Override
  public String visitExportDefinition(ExportDefinition node, Object context) {
    return "EXPORT " + node.getTablePath() + " TO " + node.getSinkPath();
  }

  @Override
  public String visitAllColumns(AllColumns node, Object context) {
    return node.getPrefix().map(p -> p + ".").orElse("") + "*";
  }

  @Override
  public String visitScript(ScriptNode node, Object context) {
    return node.getStatements()
        .stream().map(s -> s.accept(this, context))
        .collect(Collectors.joining("\n"));
  }

  @Override
  public String visitIdentifier(Identifier node, Object context) {
    return node.getNamePath().toString();
  }

  @Override
  public String visitSelectItem(SelectItem node, Object context) {
    throw new RuntimeException(
        String.format("Undefiend node in printer %s", node.getClass().getName()));
  }

  @Override
  public String visitDistinctAssignment(DistinctAssignment node, Object context) {
    return node.getNamePath() + " := " + String.format("DISTINCT %s ON %s %s;", node.getTable(),
        node.getPartitionKeys(),
        node.getOrder() != null ? String.format("ORDER BY %s", node.getOrder()) : "");
  }

  @Override
  public String visitSingleColumn(SingleColumn node, Object context) {
    return node.getExpression() + node.getAlias().map(e -> " AS " + e.accept(this, context))
        .orElse("");
  }
}
