package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.ResolvedField;
import ai.dataeng.sqml.schema.AbstractField;
import ai.dataeng.sqml.schema.Schema;
import ai.dataeng.sqml.schema.SchemaField;
import ai.dataeng.sqml.schema.SchemaObject;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.type.SqmlType;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;

public class Scope {

  private final QualifiedName path;
  private final Optional<SqmlType> type;

  public Scope(QualifiedName path) {
    this.path = path;
    this.type = Optional.empty();
  }

  public QualifiedName getPath() {
    return path;
  }

  public Optional<SqmlType> getType() {
    return type;
  }

  public ResolvedField resolveField(Expression expression, QualifiedName qualifiedName) {
    // w/ the current Path we're on in Rel
    // Traverse through the qualified name to find sub relation
    //
    return null;
  }

  public String getTypeFromSchema(QualifiedName context, String field) {
    Preconditions.checkState(context.getParts().size() > 0,
        "Sqml assignment missing first part: %s", context);
    String sourceName = context.getParts().get(0);

    Schema schema = getSchema(sourceName);

    Preconditions.checkNotNull(schema, "Schema could not be found: %s", sourceName);

    AbstractField abstractField = null;
    List<String> parts = context.getParts();
    for (int i = 1; i < parts.size() - 1; i++) {
      String part = parts.get(i);
      Optional<AbstractField> partField = schema.getField(part);
      Preconditions.checkState(partField.isPresent(), "Could not find schema field: %s in %s",
          part, context);
      abstractField = partField.get();
    }
    if (abstractField == null) {
      abstractField = schema.getField(field)
          .orElseThrow(() -> new RuntimeException(
              String.format("Could not find field %s in %s", field, context)));
    } else {
      Preconditions
          .checkState(abstractField instanceof SchemaObject, "Terminal not an object %s in %s",
              context, field);
      abstractField = ((SchemaObject) abstractField).getField(field)
          .orElseThrow(() -> new RuntimeException(
              String.format("Could not find field %s in %s", field, context)));
    }

    Preconditions.checkState(abstractField instanceof SchemaField,
        "Referenced field does not have a concrete type: %s", abstractField.getName());
    return ((SchemaField) abstractField).getType().name();
  }


  public Schema getSchema(String sourceName) {
//    Schema localSource = localSources.get(sourceName);
//    if (localSource != null) {
//      return localSource;
//    }

//    return schemas.get(sourceName);
    return null;
  }

}