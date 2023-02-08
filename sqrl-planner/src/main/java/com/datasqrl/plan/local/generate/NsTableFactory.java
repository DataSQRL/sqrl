package com.datasqrl.plan.local.generate;

import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.datasqrl.plan.calcite.table.StateChangeType;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.schema.SQRLTable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SubscriptionType;

@AllArgsConstructor
public class NsTableFactory {

  CalciteTableFactory tableFactory;

  public NamespaceObject createTable(Namespace ns, NamePath namePath, AnnotatedLP processedRel,
      Optional<SubscriptionType> subscriptionType, Optional<SQRLTable> parentTable) {
    return new SqrlTableNamespaceObject(namePath.getLast(),
        createScriptDef(ns, namePath, processedRel, subscriptionType, parentTable));
  }

  public ScriptTableDefinition createScriptDef(Namespace ns, NamePath namePath,
      AnnotatedLP processedRel, Optional<SubscriptionType> subscriptionType,
      Optional<SQRLTable> parentTable) {
    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> Name.system(n)).collect(Collectors.toList());

    if (subscriptionType.isPresent()) {
      return tableFactory.defineStreamTable(namePath,
          processedRel,
          StateChangeType.valueOf(subscriptionType.get().name()),
          ns.createRelBuilder(), ns.session.getPipeline());
    } else {

//    Optional<Pair<SQRLTable, VirtualRelationalTable>> parentPair = parentTable.map(tbl ->
//        Pair.of(tbl, env.tableMap.get(tbl)));

      return tableFactory.defineTable(namePath,
          processedRel, fieldNames, parentTable);
    }
  }
}
