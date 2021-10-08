package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.execution.importer.ImportSchema.ImportType;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.List;
import lombok.Getter;

@Getter
public class ImportRelationDefinition extends RelationDefinition {

  private final Name datasetName;
  private final ImportType type;
  private final Name tableName;
  private final QualifiedName name;
  private final RelationType relation;

  public ImportRelationDefinition(Name datasetName, ImportType type, Name tableName,
      QualifiedName name, RelationType relation) {
    this.datasetName = datasetName;
    this.type = type;
    this.tableName = tableName;
    this.name = name;
    this.relation = relation;
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitImportTableDefinition(this, context);
  }

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
    return name;
  }

  @Override
  public RelationIdentifier getRelationIdentifier() {
    return new RelationIdentifier(name);
  }
}