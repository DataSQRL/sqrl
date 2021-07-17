package ai.dataeng.sqml.model;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ModelRelation {
  private String name;
  private List<ModelRelation> fields;

  private Optional<Analysis> analysis;

  public ModelRelation(String name, List<ModelRelation> fields,
      Optional<Analysis> analysis) {
    this.name = name;
    this.fields = fields;
    this.analysis = analysis;
  }

  public String getName() {
    return name;
  }

  public List<ModelRelation> getFields() {
    return fields;
  }

  public Optional<Analysis> getAnalysis() {
    return analysis;
  }

  public void addRelation(QualifiedName name, Analysis analysis) {
    ModelRelation relation = createNestedRelations(name);

    relation.addField(name.getParts().get(name.getParts().size() - 1), Optional.ofNullable(analysis));
  }

  private ModelRelation createNestedRelations(QualifiedName name) {
    ModelRelation relation = this;
    for (int i = 0; i < name.getParts().size() - 1; i++) {
      String partName = name.getParts().get(i);

      ModelRelation child = getRelation(relation, partName);
      if (child == null) {
        relation = relation.addField(partName, Optional.empty());
      } else {
        relation = child;
      }
    }
    return relation;
  }

  private ModelRelation getRelation(ModelRelation relation, String partName) {
    for (ModelRelation field : relation.getFields()) {
      if (field.getName().equals(partName) && field instanceof ModelRelation) {
        return field;
      }
    }
    return null;
  }

  protected ModelRelation addField(String fieldName, Optional<Analysis> analysis) {
    ModelRelation modelRelation = new ModelRelation(fieldName, new ArrayList<>(), analysis);
    fields.add(modelRelation);
    return modelRelation;
  }
}
