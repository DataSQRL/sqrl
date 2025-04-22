/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.nio.file.Path;

import com.datasqrl.util.TestResources;

public class BookClub {

  public static final Path DATA_DIR = TestResources.RESOURCE_DIR.resolve("bookclub");
  public static final Path[] BOOK_FILES = new Path[]{DATA_DIR.resolve("book_001.json"),
      DATA_DIR.resolve("book_002.json")};


}
