package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.ViewQueryRewriter;
import ai.dataeng.sqml.ViewQueryRewriter.ViewTable;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

@Value
public class PhysicalModel {
  private List<ViewTable> tables;

  public PhysicalModel() {
    this.tables = new ArrayList<>();
  }

  public void addTable(ViewTable viewTable) {
    this.tables.add(viewTable);
  }
}
