package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.List;

@AllArgsConstructor
@Getter
public abstract class AddedColumn {

    final String nameId;
    //Being inlined means that the column has been added to the base QueryRelationalTable
    final boolean isInlined;

    public abstract RelDataType getDataType();


    public static class Simple extends AddedColumn {

        final RexNode expression;

        public Simple(String nameId, RexNode expression, boolean isInlined) {
            super(nameId, isInlined);
            this.expression = expression;
        }

        @Override
        public RelDataType getDataType() {
            return expression.getType();
        }

        public RexNode getExpression(IndexMap indexMap) {
            return SqrlRexUtil.mapIndexes(expression, indexMap);
        }
    }


    public static class Complex extends AddedColumn {

        //Logical plan that produces the added column value in the last field. All previous fields
        //are primary key columns of the table (in the same order as for the table) to which this column is added.
        final RelNode leftJoin;

        public Complex(String nameId, RelNode leftJoin) {
            super(nameId, false); //For now, we never inline complex columns
            this.leftJoin = leftJoin;
        }

        RelDataTypeField getAddedColumnField() {
            List<RelDataTypeField> fields = leftJoin.getRowType().getFieldList();
            return fields.get(fields.size()-1);
        }

        @Override
        public RelDataType getDataType() {
            return getAddedColumnField().getType();
        }
    }

}
