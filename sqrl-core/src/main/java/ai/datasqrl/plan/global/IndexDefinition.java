package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.google.common.collect.ContiguousSet;
import lombok.Value;

import java.util.List;
import java.util.stream.Collectors;

@Value
public class IndexDefinition {

    public static final String INDEX_NAME = "_index_";

    public enum Type {
      HASH, BTREE;

      public boolean hasStrictColumnOrder() {
          return this==HASH || this==BTREE;
      }

      public boolean requiresAllColumns() {
          return this==HASH;
      }

      public boolean supports(IndexCall.CallType callType) {
          switch (this) {
              case HASH:
                  return callType == IndexCall.CallType.EQUALITY;
              case BTREE:
                  return callType == IndexCall.CallType.EQUALITY || callType == IndexCall.CallType.COMPARISON;
              default:
                  throw new IllegalStateException(this.name());
          }
      }

    }

    VirtualRelationalTable table;
    List<Integer> columns;
    Type type;


    public String getName() {
        return table.getNameId() + "_" + type.name().toLowerCase() + "_" +
                columns.stream().map(i -> "c"+i).collect(Collectors.joining());
    }

    public static IndexDefinition getPrimaryKeyIndex(VirtualRelationalTable table) {
        return new IndexDefinition(table, ContiguousSet.closedOpen(0,table.getNumPrimaryKeys()).asList(),Type.BTREE);
    }

}
