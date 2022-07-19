package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.CalciteUtil.RelDataTypeFieldBuilder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;

@Getter
public class QuerySqrlTable extends AbstractSqrlTable {

  @NonNull
  private final Type type;
  @NonNull
  private final TimestampHolder timestamp;
  @NonNull
  private final int numPrimaryKeys;
  //added through expressions, queries
  private final List<RelDataTypeField> addedFields = new ArrayList<>();
  //    @NonNull private final RelNode relNode;
  RelDataType relDataType;
  @Setter
  private TableStatistic statistic = null;

  public QuerySqrlTable(@NonNull Name rootTableId, @NonNull Type type,
      @NonNull RelDataType relDataType,
      @NonNull TimestampHolder timestamp,
      @NonNull int numPrimaryKeys) {
    super(getTableId(rootTableId));
    this.type = type;
    this.relDataType = relDataType;
    this.timestamp = timestamp;
    this.numPrimaryKeys = numPrimaryKeys;
  }

  public static Name getTableId(Name rootTableId) {
    return rootTableId.suffix("Q");
  }

  private static RelDataTypeField getField(FieldIndexPath path, RelDataType rowType) {
    Preconditions.checkArgument(path.getLength() > 0);
    Preconditions.checkArgument(rowType.isStruct(), "Expected relational data type but found: %s",
        rowType);
    int firstIndex = path.get(0);
    Preconditions.checkArgument(firstIndex < rowType.getFieldCount());
    RelDataTypeField field = rowType.getFieldList().get(firstIndex);
    path = path.popFirst();
    if (path.isEmpty()) {
      return field;
    } else {
      return getField(path, field.getType());
    }
  }

  @Override
  public RelDataType getRowType() {
    RelDataTypeFieldBuilder fieldBuilder = new RelDataTypeFieldBuilder(new FieldInfoBuilder(new SqrlTypeFactory(new SqrlTypeSystem())));
    relDataType.getFieldList().forEach(fieldBuilder::add);
    addedFields.forEach(fieldBuilder::add);
    return fieldBuilder.build();
  }

  @Override
  public void addField(RelDataTypeField relDataTypeField) {
    addedFields.add(relDataTypeField);
  }

  public boolean isPrimaryKey(FieldIndexPath path) {
    Preconditions.checkArgument(path.getLength() > 0);
    if (path.getLength() == 1) {
      return path.get(0) < numPrimaryKeys;
    } else {
      if (path.getLast() != 0) {
        return false;
      }
      RelDataType type = getField(path.popLast()).getType();
      return CalciteUtil.isNestedTable(type) && CalciteUtil.isArray(type);
    }
  }

  public RelDataTypeField getField(FieldIndexPath path) {
    return getField(path, getRowType());
  }

  @Override
  public Statistic getStatistic() {
    if (statistic != null) {
      return Statistics.of(statistic.getRowCount(), List.of(ImmutableBitSet.of(numPrimaryKeys)));
    } else {
      return Statistics.UNKNOWN;
    }
  }


  public enum Type {
    STREAM, //a stream of records with synthetic (i.e. uuid) primary key ordered by timestamp
    TEMPORAL_STATE, //table with natural primary key that ensures uniqueness and timestamp for
    // change-stream
    STATE //table with natural primary key that ensures uniqueness but no timestamp (i.e.
    // represents timeless state)
  }

}
