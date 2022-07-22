package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.schema.builder.VirtualTable;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

@Getter
public abstract class VirtualSqrlTable extends AbstractSqrlTable implements VirtualTable {

  protected final int numLocalPks;
  @NonNull
  protected final List<AddedColumn> addedColumns = new ArrayList<>();
  /**
   * The data type for the (possibly shredded) table represented by this virtual table
   */
  @NonNull
  protected RelDataType rowType;
  /**
   * The row type of the underlying {@link QuerySqrlTable} at the shredding level of this virtual
   * table including any nested relations. This is distinct from the rowType of the virtual table
   * which is padded with parent primary keys and does not contain nested relations.
   */
  @NonNull
  protected RelDataType queryRowType;

  protected VirtualSqrlTable(Name nameId, @NonNull RelDataType rowType,
      @NonNull RelDataType queryRowType, int numLocalPks) {
    super(nameId);
    this.rowType = rowType;
    this.queryRowType = queryRowType;
    this.numLocalPks = numLocalPks;
  }

  public abstract boolean isRoot();

  public abstract VirtualSqrlTable.Root getRoot();

  public void addColumn(@NonNull AddedColumn column, @NonNull RelDataTypeFactory typeFactory) {
    //Ensure that all previously added columns are inlined if the column to be added is
    Preconditions.checkArgument(
        !column.isInlined || addedColumns.stream().allMatch(AddedColumn::isInlined),
        "Cannot add inlined column when previous columns aren't inlined: %s", addedColumns);
    addedColumns.add(column);
    //Update the row types
    RelDataType colType = column.getDataType();
    if (!CalciteUtil.isNestedTable(colType)) {
      rowType = CalciteUtil.appendField(rowType, column.getNameId(), column.getDataType(),
          typeFactory);
    }
    queryRowType = CalciteUtil.appendField(queryRowType, column.getNameId(), column.getDataType(),
        typeFactory);
  }

  public abstract int getNumParentPks();

  public int getNumPrimaryKeys() {
    return getNumParentPks() + numLocalPks;
  }

  public int getNumColumns() {
    return rowType.getFieldCount();
  }

  public int getNumQueryColumns() {
    return queryRowType.getFieldCount();
  }


  @Getter
  public static class Root extends VirtualSqrlTable {

    @NonNull
    final QuerySqrlTable base;

    protected Root(Name nameId, @NonNull RelDataType rowType, @NonNull QuerySqrlTable base) {
      super(nameId, rowType, base.getRowType(), base.getNumPrimaryKeys());
      this.base = base;
    }

    @Override
    public boolean isRoot() {
      return true;
    }

    @Override
    public Root getRoot() {
      return this;
    }

    @Override
    public int getNumParentPks() {
      return 0;
    }
  }

  @Getter
  public static class Child extends VirtualSqrlTable {

    @NonNull
    final VirtualSqrlTable parent;
    final int shredIndex;

    private Child(Name nameId, @NonNull RelDataType rowType, @NonNull VirtualSqrlTable parent,
        int shredIndex, RelDataType queryDataType) {
      super(nameId, rowType, CalciteUtil.getArrayElementType(queryDataType).orElse(queryDataType),
          //unwrap if type is in array
          CalciteUtil.getArrayElementType(queryDataType).isPresent() ? 1 : 0);
      //We currently make the hard-coded assumption that children have at most one local primary
      // key column
      this.parent = parent;
      this.shredIndex = shredIndex;
    }

    public static Child of(Name nameId, @NonNull RelDataType rowType,
        @NonNull VirtualSqrlTable parent, @NonNull String shredFieldName) {
      RelDataTypeField shredField = parent.getQueryRowType().getField(shredFieldName, true, false);
      Preconditions.checkArgument(shredField != null);
      RelDataType type = shredField.getType();
      Preconditions.checkArgument(CalciteUtil.isNestedTable(type));
      type = CalciteUtil.getArrayElementType(type).orElse(type);
      Child child = new Child(nameId, rowType, parent, shredField.getIndex(), type);
      return child;
    }

    @Override
    public boolean isRoot() {
      return false;
    }

    @Override
    public Root getRoot() {
      return parent.getRoot();
    }

    @Override
    public void addColumn(@NonNull AddedColumn column, @NonNull RelDataTypeFactory typeFactory) {
      //It's not currently supported to inline child columns since that requires un- and
      // re-collecting tables
      //which introduces a lot of complexity that we currently avoid because the benefit doesn't
      // seem high
      Preconditions.checkArgument(!column.isInlined, "Cannot inline column on child tables");
      super.addColumn(column, typeFactory);
    }

    @Override
    public int getNumParentPks() {
      return parent.getNumPrimaryKeys();
    }
  }

}
