package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.schema.builder.VirtualTable;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A relational table that represents the relational equivalent of a {@link ai.datasqrl.schema.SQRLTable}.
 *
 * While the SQRLTable represents the logical table that a user defines in an SQRL script, the associated
 * {@link VirtualRelationalTable} represents the relational representation of that same table (i.e. with
 * primary keys, relational data type, etc). The transpiler manages the mapping between the two and updates
 * both according to the script statements. Specifically, relationships on SQRLTables are converted to
 * JOINs between virtual tables.
 */
@Getter
public abstract class VirtualRelationalTable extends AbstractRelationalTable implements VirtualTable {

  protected final int numLocalPks;
  @NonNull
  protected final List<AddedColumn> addedColumns = new ArrayList<>();
  /**
   * The data type for the (possibly shredded) table represented by this virtual table
   */
  @NonNull
  protected RelDataType rowType;
  /**
   * The row type of the underlying {@link QueryRelationalTable} at the shredding level of this virtual
   * table including any nested relations. This is distinct from the rowType of the virtual table
   * which is padded with parent primary keys and does not contain nested relations.
   */
  @NonNull
  protected RelDataType queryRowType;

  /*
  The pullups that should be executed in the database (i.e. after persistence) as determined
  by the Optimizer. This is used to correctly expand the table in the DAG cutting.
   */
  @Setter
  private PullupOperator.Container dbPullups = PullupOperator.Container.EMPTY;

  protected VirtualRelationalTable(Name nameId, @NonNull RelDataType rowType,
                                   @NonNull RelDataType queryRowType, int numLocalPks) {
    super(nameId);
    this.rowType = rowType;
    this.queryRowType = queryRowType;
    this.numLocalPks = numLocalPks;
  }

  public abstract boolean isRoot();

  public abstract VirtualRelationalTable.Root getRoot();

  public void addColumn(@NonNull AddedColumn column, @NonNull RelDataTypeFactory typeFactory,
                        Supplier<RelBuilder> relBuilderFactory, Optional<Integer> timestampScore) {
    if (isRoot() && column instanceof AddedColumn.Simple && addedColumns.isEmpty()) {
      //We can inline this column on the parent table
      ((VirtualRelationalTable.Root)this).getBase().addInlinedColumn((AddedColumn.Simple) column,
              relBuilderFactory, timestampScore);
    } else {
      addedColumns.add(column);
    }
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

  public List<String> getPrimaryKeyNames() {
    List<String> pkNames = new ArrayList<>(getNumPrimaryKeys());
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (int i = 0; i < getNumPrimaryKeys(); i++) {
      pkNames.add(i,fields.get(i).getName());
    }
    return pkNames;
  }

  public int getNumColumns() {
    return rowType.getFieldCount();
  }

  public int getNumQueryColumns() {
    return queryRowType.getFieldCount();
  }


  @Getter
  public static class Root extends VirtualRelationalTable {

    @NonNull
    final QueryRelationalTable base;

    protected Root(Name nameId, @NonNull RelDataType rowType, @NonNull QueryRelationalTable base) {
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
  public static class Child extends VirtualRelationalTable {

    @NonNull
    final VirtualRelationalTable parent;
    final int shredIndex;

    private Child(Name nameId, @NonNull RelDataType rowType, @NonNull VirtualRelationalTable parent,
        int shredIndex, RelDataType queryDataType, int numLocalPks) {
      super(nameId, rowType, queryDataType, numLocalPks);
      this.parent = parent;
      this.shredIndex = shredIndex;
    }

    public static Child of(Name nameId, @NonNull RelDataType rowType,
                           @NonNull VirtualRelationalTable parent, @NonNull String shredFieldName) {
      RelDataTypeField shredField = parent.getQueryRowType().getField(shredFieldName, true, false);
      Preconditions.checkArgument(shredField != null);
      RelDataType type = shredField.getType();
      Preconditions.checkArgument(CalciteUtil.isNestedTable(type));
      //We currently make the hard-coded assumption that children have at most one local primary
      int numLocalPks = CalciteUtil.getArrayElementType(type).isPresent() ? 1 : 0;
      //unwrap if type is in array
      type = CalciteUtil.getArrayElementType(type).orElse(type);
      Child child = new Child(nameId, rowType, parent, shredField.getIndex(), type, numLocalPks);
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
    public int getNumParentPks() {
      return parent.getNumPrimaryKeys();
    }
  }

}
