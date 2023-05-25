package com.datasqrl.loaders;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.TableNamespaceObject;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TableSourceNamespaceObject implements TableNamespaceObject<TableSource>, TableSourceObject {

  private final TableSource table;


  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public TableSource getSource() {
    return table;
  }
}
