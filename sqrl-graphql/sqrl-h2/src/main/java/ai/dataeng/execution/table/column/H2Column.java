package ai.dataeng.execution.table.column;

import ai.dataeng.execution.table.H2ColumnVisitor2;
import java.util.Set;
import lombok.Value;

public interface H2Column {
  public String getName();
  public String getPhysicalName();

  public <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context);
}
