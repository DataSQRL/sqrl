package ai.dataeng.sqml;

import ai.dataeng.sqml.plan.PlanNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ModelRelation {
  private String name;
  private Optional<PlanNode> plan;
  private List<ModelRelation> fields;

  public ModelRelation(String name, Optional<PlanNode> plan) {
    this.name = name;
    this.plan = plan;
    this.fields = new ArrayList<>();
  }

  public List<ModelRelation> getFields() {
    return fields;
  }

  protected String getName() {
    return name;
  }

  public Optional<PlanNode> getPlan() {
    return plan;
  }

  protected ModelRelation addField(String fieldName, PlanNode plan) {
    ModelRelation modelRelation = new ModelRelation(fieldName, Optional.ofNullable(plan));
    fields.add(modelRelation);
    return modelRelation;
  }

  @Override
  public String toString() {
    return "ModelRelation{" +
        "name='" + name + '\'' +
        ", plan=" + plan +
        ", fields=" + fields +
        '}';
  }
}