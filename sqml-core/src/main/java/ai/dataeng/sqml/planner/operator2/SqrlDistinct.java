package ai.dataeng.sqml.planner.operator2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.name.Name;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.SqrlRelBuilder;
import org.apache.calcite.util.Util;

/**
 * A special Distinct Sqrl node. Distinct operations set a new primary key based on the
 *  partition information. Distinct operations do not inherit relationships. Shadowed fields
 *  are not inherited.
 *
 *
 * When translating:
 * We generate a row_number & filter to get distinct elements. We may be able to use a
 * DISTINCT ON(...) clause in SQL but support does not exist in flink.
 */
public class SqrlDistinct extends SingleRel implements SqrlRelNode {

  private final List<Field> fields;
  private final List<Name> partition;
  private final List<SortItem> order;
  private final List<RexInputRef> partitionRef;
  private final RelCollation collation;

  public SqrlDistinct(RelOptCluster cluster, RelTraitSet traits,
      List<RelHint> hints, RelNode input,
      List<Name> partition, List<SortItem> order,
      List<RexInputRef> partitionRef, RelCollation orderCollation) {
    super(cluster, traits, input);
    this.order = order;
    this.partitionRef = partitionRef;
    this.collation = orderCollation;
    this.fields = generateFields(input, partition);
    this.partition = partition;
  }

  private List<Field> generateFields(RelNode input, List<Name> partition) {
    List<Field> newFields = new ArrayList<>();
    SqrlRelNode rel = (SqrlRelNode) input;
    for (int i = 0; i < rel.getFields().size(); i++) {
      Field field = rel.getFields().get(i);
      if (field instanceof Column) {
        Column column = (Column) field.copy();
        column.setPrimaryKey(partition.contains(column.getName()));
        column.setForeignKey(false);
        newFields.add(column);
      }
    }

    return newFields;
  }

  public SqrlDistinct copy(RelTraitSet traitSet, RelNode input,
      List<Name> partition, RelDataType rowType) {
    return new SqrlDistinct(getCluster(), traitSet, null, input, partition, order, partitionRef,
        collation);
  }

  /** Creates a SqrlDistinct. */
  public static SqrlDistinct create(final RelNode input, List<RelHint> hints,
      List<Name> partition, List<SortItem> order, List<RexInputRef> partitionRef,
      RelCollation orderCollation) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(Convention.NONE);
//            .replaceIfs(RelCollationTraitDef.INSTANCE,
//                () -> RelMdCollation.project(mq, input, partition));
    return new SqrlDistinct(cluster, traitSet, hints, input, partition, order, partitionRef, orderCollation);
  }

  @Override
  public List<Field> getFields() {
    return this.fields;
  }

  @Override
  public List<Column> getPrimaryKeys() {
    return this.fields.stream()
        .filter(c->c instanceof Column)
        .map(c->(Column)c)
        .filter(c->c.isPrimaryKey)
        .collect(Collectors.toList());
  }

  @Override
  public List<Column> getReferences() {
    return null;
  }

  @Override
  protected RelDataType deriveRowType() {
    final RelOptCluster cluster = input.getCluster();
    List<RexNode> fields = new ArrayList<>();
    List<String> names = new ArrayList<>();
    SqrlRelNode node = (SqrlRelNode)this.input;

    for (Field field : this.fields) {
      int parentIndex = SqrlRelBuilder.getIndex(node.getFields(), field.getName());
      RexInputRef input = RexInputRef.of(parentIndex, this.input.getRowType());
      fields.add(input);
      names.add(field.getName().toString());
    }

    RelDataType type =  RexUtil.createStructType(cluster.getTypeFactory(), fields,
        names, SqlValidatorUtil.F_SUGGESTER);

    return type;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.item("on", this.partitionRef);

    for (Ord<RexNode> ord : Ord.zip(getSortExps())) {
      pw.item("sort" + ord.i, ord.e);
    }
    for (Ord<RelFieldCollation> ord
        : Ord.zip(collation.getFieldCollations())) {
      pw.item("dir" + ord.i, ord.e.shortString());
    }

    return pw;
  }

  private List<RexNode> getSortExps() {
    return Util.transform(this.collation.getFieldCollations(), field ->
        getCluster().getRexBuilder().makeInputRef(input,
            Objects.requireNonNull(field, "field").getFieldIndex()));
  }
}
