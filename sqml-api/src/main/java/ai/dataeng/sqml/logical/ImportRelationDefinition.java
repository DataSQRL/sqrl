package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.List;
import lombok.Getter;

@Getter
public class ImportRelationDefinition extends RelationDefinition {

  private final RelationType relation;
  private final ImportSchema schema;

  public ImportRelationDefinition(ImportSchema schema) {
    this.schema = schema;
    this.relation = schema.getSchema();
  }

//  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
//    return visitor.visitImportTableDefinition(this, context);
//  }

  @Override
  protected List<Field> getPrimaryKeys() {
    return relation.getFields();
  }

  @Override
  public List<Field> getFields() {
    return relation.getFields();
  }

  @Override
  public QualifiedName getRelationName() {
    return QualifiedName.of("");
  }

  @Override
  public RelationIdentifier getRelationIdentifier() {
    return new RelationIdentifier(QualifiedName.of(""));
  }
}