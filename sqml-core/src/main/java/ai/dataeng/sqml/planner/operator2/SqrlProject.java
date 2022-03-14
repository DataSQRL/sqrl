package ai.dataeng.sqml.planner.operator2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.RowToColumnConverter;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqrlProject extends Project implements SqrlRelNode {

  private final List<Field> fields;

  public SqrlProject(RelOptCluster cluster,
      RelTraitSet traits, List<RelHint> hints,
      RelNode input, List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traits, hints, input, projects, rowType);
    this.fields = generateFields(projects);
  }

  private List<Field> generateFields(List<? extends RexNode> projects) {
    RelNode in = this.input;
    if (this.input instanceof HepRelVertex) {
      in = ((HepRelVertex) this.input).getCurrentRel();
    }
    List<Field> newFields = new ArrayList<>();

    //The index of the projection is the same as its rowType
    for (int i = 0; i < projects.size(); i++) {
      RexNode p = projects.get(i);
      if (p instanceof RexInputRef) {
        newFields.add(((SqrlRelNode) in).getFields().get(((RexInputRef) p).getIndex()));
      } else {
        newFields.add(Column.createTemp(this.getRowType().getFieldNames().get(i),
            RowToColumnConverter.toBasicType(this.getRowType().getFieldList().get(i).getType()),
            null));
      }
    }

    return newFields;
  }

  @Override public SqrlProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new SqrlProject(getCluster(), traitSet, hints, input, projects, rowType);
  }


  /** Creates a SqrlProject. */
  public static SqrlProject create(final RelNode input, List<RelHint> hints,
      final List<? extends RexNode> projects,
      @Nullable List<? extends @Nullable String> fieldNames) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames, SqlValidatorUtil.F_SUGGESTER);
    return create(input, hints, projects, rowType);
  }

  /** Creates a SqrlProject, specifying row type rather than field names. */
  public static SqrlProject create(final RelNode input, List<RelHint> hints,
      final List<? extends RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.project(mq, input, projects));
    return new SqrlProject(cluster, traitSet, hints, input, projects, rowType);
  }

  @Override
  public List<Field> getFields() {
    return fields;
  }

  @Override
  public List<Column> getPrimaryKeys() {
    return ((SqrlRelNode)input).getPrimaryKeys();
  }

  @Override
  public List<Column> getReferences() {
    return null;
  }
}
