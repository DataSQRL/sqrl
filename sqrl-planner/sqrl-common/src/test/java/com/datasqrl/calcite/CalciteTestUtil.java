package com.datasqrl.calcite;

import com.datasqrl.calcite.testTables.EntriesTable;
import com.datasqrl.calcite.testTables.OrdersTable;
import com.datasqrl.calcite.testTables.ProductTable;

public class CalciteTestUtil {

  public static SqrlFramework createEcommerceFramework() {
    SqrlFramework framework = new SqrlFramework();

    framework.getSchema()
        .add("ENTRIES$", new EntriesTable());

    framework.getSchema()
        .add("ORDERS$", new OrdersTable());

    framework.getSchema()
        .add("PRODUCT$", new ProductTable());
    return framework;
  }
}
