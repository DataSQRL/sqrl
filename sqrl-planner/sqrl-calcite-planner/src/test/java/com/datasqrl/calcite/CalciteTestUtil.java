package com.datasqrl.calcite;

import com.datasqrl.calcite.testTables.EntriesTable;
import com.datasqrl.calcite.testTables.Orders;
import com.datasqrl.calcite.testTables.Product;

public class CalciteTestUtil {

  public static SqrlFramework createEcommerceFramework() {
    SqrlFramework framework = new SqrlFramework();

    framework.getSchema()
        .add("ENTRIES$", new EntriesTable());

    framework.getSchema()
        .add("ORDERS$", new Orders());

    framework.getSchema()
        .add("PRODUCT$", new Product());
    return framework;
  }
}
