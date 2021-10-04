package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.List;
import lombok.Getter;

@Getter
public class ImportRelationDefinition extends RelationDefinition {

  private final ImportDefinition expression;
  private final RelationType relation;
  private final RelationIdentifier relationIdentifier;
  private final SourceTable sourceTable;
  private final SourceTableStatistics stats;

  public ImportRelationDefinition(ImportDefinition expression, RelationType relation,
      RelationIdentifier relationIdentifier, SourceTable sourceTable,
      SourceTableStatistics stats) {
    this.expression = expression;
    this.relation = relation;
    this.relationIdentifier = relationIdentifier;
    this.sourceTable = sourceTable;
    this.stats = stats;
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
    return relationIdentifier.getName();
  }
}