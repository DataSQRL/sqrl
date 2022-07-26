package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

@Getter
public class QuerySqrlTable extends AbstractSqrlTable {

  @NonNull
  private final Type type;
  @NonNull
  private final TimestampHolder.Base timestamp;
  @NonNull
  private final TopNConstraint topN;
  @NonNull
  private final int numPrimaryKeys;
  //added through expressions, queries
  private final List<AddedColumn.Simple> addedFields = new ArrayList<>();

  protected RelNode relNode;
  @Setter
  private TableStatistic statistic = null;

  public QuerySqrlTable(@NonNull Name rootTableId, @NonNull Type type,
      RelNode relNode,
      @NonNull TimestampHolder.Base timestamp, @NonNull TopNConstraint topN,
      @NonNull int numPrimaryKeys) {
    super(rootTableId);
    this.type = type;
    this.timestamp = timestamp;
    this.topN = topN;
    this.relNode = relNode;
    this.numPrimaryKeys = numPrimaryKeys;
  }

  public RelNode getRelNode() {
    Preconditions.checkState(relNode!=null,"Not yet initialized");
    return relNode;
  }

  private static RelDataTypeField getField(FieldIndexPath path, RelDataType rowType) {
    Preconditions.checkArgument(path.size() > 0);
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
    return relNode.getRowType();
  }

  public boolean isPrimaryKey(FieldIndexPath path) {
    Preconditions.checkArgument(path.size() > 0);
    if (path.size() == 1) {
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
