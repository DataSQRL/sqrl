package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.ResolvedField;
import ai.dataeng.sqml.expression.ExpressionAnalysis;
import ai.dataeng.sqml.expression.ExpressionAnalyzer;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Analyzer {
  private final Metadata metadata;
  private final Analysis analysis;
  private final Script script;
  private final List<QuerySpecification> var = null;

  private Analyzer(Script script, Metadata metadata, Analysis analysis) {
    this.script = script;
    this.metadata = metadata;
    this.analysis = analysis;
  }

  public static Analysis analyze(Script script, Metadata metadata) {
    Analysis analysis = new Analysis(script);
    Analyzer analyzer = new Analyzer(script, metadata, analysis);

    analyzer.analyze();

    return analysis;
  }

  public void analyze() {
    Visitor visitor = new Visitor();
    script.accept(visitor, Optional.empty());
  }

  class Visitor extends DefaultTraversalVisitor<Scope, Optional<Scope>> {
    Map<QualifiedName, SqmlType> types = new HashMap<>();
    Map<QualifiedName, Map<String, ResolvedField>> columns = new HashMap<>();
    @Override
    protected Scope visitAssign(Assign node, Optional<Scope> context) {


//      QuerySpecification rel = createOrGetRelation(node.getName());
//      Scope scope = new Scope(node.getName(), rel);
//      node.getRhs().accept(this, Optional.of(scope));
      return null;
    }

//
//    private QuerySpecification createOrGetRelation(QualifiedName name) {
//      QuerySpecification specification = getOrCreateRootRelation(name.getParts().get(0));
//
//      List<SelectItem> items = specification.getSelect().getSelectItems();
//      List<String> parts = name.getParts();
//      for (int i = 1; i < parts.size() - 1; i++) {
//        String part = parts.get(i);
//        items = getOrCreateSelectItem(items, part);
//      }
//
//      items.add(new SingleColumn(new Identifier(name.getParts().get(name.getParts().size() - 1))));
//
//      return specification;
//    }
//
//    private QuerySpecification getOrCreateRootRelation(String name) {
//      QuerySpecification spec = rootRelations.get(name);
//      if (spec == null) {
//        QuerySpecification s = new QuerySpecification(
//            new Select(false, new ArrayList<>()));
//        rootRelations.put(name, s);
//        return s;
//      }
//      return spec;
//    }
//
//    private List<SelectItem> getOrCreateSelectItem(List<SelectItem> items, String part) {
//      for (SelectItem item : items) {
//        if (item instanceof NestedSelectItem) {
//          NestedSelectItem col = (NestedSelectItem) item;
//          if (col.getName().equalsIgnoreCase(part)) {
//            return col.getColumns();
//          }
//        }
//      }
//
//      NestedSelectItem nested = new NestedSelectItem(part);
//      items.add(nested);
//      return nested.getColumns();
//    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        Optional<Scope> context) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis analysis = analyzer.analyze(expressionAssignment.getExpression(), context.get());
      addTypeToModel(context.get().getPath(), analysis.getType());

      return null;
    }

    private void addTypeToModel(QualifiedName name, Type type) {
//      this.typeMap.put(name, type);
    }

    @Override
    protected Scope visitImport(Import node, Optional<Scope> context) {
      return super.visitImport(node, context);
    }

    @Override
    protected Scope visitIdentifier(Identifier node, Optional<Scope> context) {
      Scope scope = context.orElseThrow();
      return new Scope(scope.getPath(), /*SqmlType.of(node.getTypeHint()),*/ scope.getRel());
    }
  }
}
