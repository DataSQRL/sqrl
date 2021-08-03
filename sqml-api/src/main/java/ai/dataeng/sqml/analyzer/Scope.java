package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.ResolvedField;
import ai.dataeng.sqml.schema.AbstractField;
import ai.dataeng.sqml.schema.Schema;
import ai.dataeng.sqml.schema.SchemaField;
import ai.dataeng.sqml.schema.SchemaObject;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;

public class Scope {
  private final QualifiedName name;
  private Scope parent;
  private Node node;
  private RelationSqmlType relationType;

  public Scope(QualifiedName name) {
    this.name = name;
  }

  public Scope(QualifiedName name, Scope parent, Node node, RelationSqmlType relationType) {
    this.name = name;
    this.parent = parent;
    this.node = node;
    this.relationType = relationType;
  }

  public static Scope.Builder builder() {
    return new Builder();
  }

//  public Optional<RelationSqmlType> resolveOrCreateRel(QualifiedName name) {
//    return null;
//  }

  public static class Builder {

    private Scope parent;
    private Node node;
    private RelationSqmlType relationType;
    private QualifiedName name;

    public Builder withName(QualifiedName name) {
      this.name = name;
      return this;
    }
    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(Node node,
        RelationSqmlType relationType) {
      this.node = node;
      this.relationType = relationType;
      return this;
    }

    public Scope build() {
      return new Scope(name, parent, node, relationType);
    }
  }

  public QualifiedName getName() {
    return name;
  }
//
//  public String getTypeFromSchema(QualifiedName context, String field) {
//    Preconditions.checkState(context.getParts().size() > 0,
//        "Sqml assignment missing first part: %s", context);
//    String sourceName = context.getParts().get(0);
//
//    Schema schema = getSchema(sourceName);
//
//    Preconditions.checkNotNull(schema, "Schema could not be found: %s", sourceName);
//
//    AbstractField abstractField = null;
//    List<String> parts = context.getParts();
//    for (int i = 1; i < parts.size() - 1; i++) {
//      String part = parts.get(i);
//      Optional<AbstractField> partField = schema.getField(part);
//      Preconditions.checkState(partField.isPresent(), "Could not find schema field: %s in %s",
//          part, context);
//      abstractField = partField.get();
//    }
//    if (abstractField == null) {
//      abstractField = schema.getField(field)
//          .orElseThrow(() -> new RuntimeException(
//              String.format("Could not find field %s in %s", field, context)));
//    } else {
//      Preconditions
//          .checkState(abstractField instanceof SchemaObject, "Terminal not an object %s in %s",
//              context, field);
//      abstractField = ((SchemaObject) abstractField).getField(field)
//          .orElseThrow(() -> new RuntimeException(
//              String.format("Could not find field %s in %s", field, context)));
//    }
//
//    Preconditions.checkState(abstractField instanceof SchemaField,
//        "Referenced field does not have a concrete type: %s", abstractField.getName());
//    return ((SchemaField) abstractField).getType().name();
//  }
//
//
//  public Schema getSchema(String sourceName) {
////    Schema localSource = localSources.get(sourceName);
////    if (localSource != null) {
////      return localSource;
////    }
//
////    return schemas.get(sourceName);
//    return null;
//  }

  public RelationSqmlType getRelationType() {
    return relationType;
  }
}