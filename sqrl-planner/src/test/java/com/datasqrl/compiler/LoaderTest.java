/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.compiler;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.util.TestDataset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class LoaderTest {
//
//  @ParameterizedTest
//  @ArgumentsSource(TestDataset.AllProvider.class)
//  public void testLoadingSources(TestDataset example) {
//    ErrorCollector errors = ErrorCollector.root();
//    TableLoader loader = new TableLoader();
//    for (String tblName : example.getTables()) {
//      Optional<TableSource> table = loader.readTable(example.getRootPackageDirectory(),
//          NamePath.of(example.getName(), tblName), errors);
//      assertFalse(errors.isFatal(), errors.toString());
//      assertTrue(table.isPresent());
//      assertEquals(table.get().getName(), Name.system(tblName));
//    }
//  }

}
