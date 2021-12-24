package org.apache.calcite.rules;

import ai.dataeng.sqml.logical4.UnflattenToTree;
import ai.dataeng.sqml.logical4.UnflattenToTree.UnflattenToTreeRoot;
import ai.dataeng.sqml.logical4.LogicalPlan.DatasetOrTable;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.CalciteSqrlTable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rules.SqrlTableExpanderRule.Config;

public class SqrlTableExpanderRule
      extends RelRule<Config>
      implements TransformationRule {


    public SqrlTableExpanderRule(Config config) {
        super(config);
    }




    @Override public void onMatch(RelOptRuleCall call) {
      LogicalTableScan tableScan = call.rel(0);
      CalciteSqrlTable table = tableScan.getTable().unwrap(CalciteSqrlTable.class);
      DatasetOrTable sqrlTable = table.getTable();


      //TODO: Remove this
      UnflattenToTree<Name> tablePath = new UnflattenToTreeRoot<>();
      tablePath.append(NamePath.parse(table.getPath()));

      UnflattenToTree<Name> fields = new UnflattenToTreeRoot<>();
      for (RelDataTypeField field : table.getHolder().getFieldList()) {
        fields.append(NamePath.parse(field.getName()));
      }
      System.out.println();
      System.out.println();

//      call.transformTo(call.rel(0).getInput(0));
    }
//  int Foo = 0;
//  int Foo() {return 0;}
    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY
          .withOperandSupplier(b ->
              b.operand(TableScan.class).anyInputs())
          .as(Config.class);

      @Override default SqrlTableExpanderRule toRule() {
        return new SqrlTableExpanderRule(this);
      }
    }
  }