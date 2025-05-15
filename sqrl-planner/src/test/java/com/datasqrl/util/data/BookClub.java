/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.util.data;

import com.datasqrl.util.TestResources;
import java.nio.file.Path;

public class BookClub {

  public static final Path DATA_DIR = TestResources.RESOURCE_DIR.resolve("bookclub");
  public static final Path[] BOOK_FILES =
      new Path[] {DATA_DIR.resolve("book_001.json"), DATA_DIR.resolve("book_002.json")};
}
