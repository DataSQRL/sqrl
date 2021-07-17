package ai.dataeng.sqml.rewrite;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.registry.ScriptRegistry;
import ai.dataeng.sqml.schema.AbstractField;
import ai.dataeng.sqml.schema.Schema;
import ai.dataeng.sqml.schema.SchemaField;
import ai.dataeng.sqml.schema.SchemaObject;
import ai.dataeng.sqml.schema.SchemaVisitor;
import ai.dataeng.sqml.source.Source;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Script;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ResolveImportStatements extends ScriptRewrite {

  private final Metadata metadata;

  public ResolveImportStatements(Metadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public Script rewrite(Script script) {
    List<Node> newStatements = new ArrayList<>();
    rewrite(script, newStatements);
    return new Script(script.getLocation(), newStatements);
  }

  public void rewrite(Script script, List<Node> newStatements) {
    for (Node node : script.getStatements()) {
      if (node instanceof Import) {
        newStatements.add(node);
        visitImport((Import)node, newStatements);
      } else {
        newStatements.add(node);
      }
    }
  }

  protected void visitImport(Import node, List<Node> newStatements) {
    switch (node.getType()) {
      case FUNCTION:
        //no-op
        break;
      case SOURCE:
        Schema source = metadata.getSchemaProvider().getSchema(node.getQualifiedName().toString());
        AddColumnFromSchema addColumnFromSchema = new AddColumnFromSchema(newStatements);
        source.accept(addColumnFromSchema, null);
        break;
      case SCRIPT:
        //TODO: TEST FOR CYCLIC IMPORTS
        Script script = metadata.getScript(node.getQualifiedName().toString());
        Preconditions.checkNotNull(script,
            String.format("Could not find script: ", node.getQualifiedName()));
        rewrite(script, newStatements);
        break;
    }
  }

  private class AddColumnFromSchema extends SchemaVisitor<Object, List<String>> {

    private final List<Node> newStatements;

    public AddColumnFromSchema(List<Node> newStatements) {
      this.newStatements = newStatements;
    }

    public Object visitSchema(Schema schema, List<String> context) {
      for (AbstractField abstractField : schema.getFields()) {
        ArrayList list = new ArrayList<>();
        list.add(schema.getName());
        abstractField.accept(this, list);
      }
      return null;
    }

    public Object visitField(SchemaField schemaField, List<String> context) {
      List<String> clone = new ArrayList<>(context);
      clone.add(schemaField.getName());
      QualifiedName name = QualifiedName.of(clone);
      Assign assign = new Assign(null, name,
          new ExpressionAssignment(Optional.empty(),
              new Identifier(name.getSuffix(), schemaField.getType().toString())));
      newStatements.add(assign);
      return null;
    }

    public Object visitObject(SchemaObject schemaObject, List<String> context) {
      for (AbstractField abstractField : schemaObject.getFields()) {
        List<String> clone = new ArrayList<>(context);
        clone.add(schemaObject.getName());
        abstractField.accept(this, clone);
      }
      return null;
    }
  }
}
