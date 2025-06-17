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

import java.nio.file.Path;
import java.util.Set;

public class Books extends UseCaseExample {

  public static final Books INSTANCE = new Books();

  public static final Path DATA_DIRECTORY = Path.of("/Users/matthias/Data/books/sample");

  protected Books() {
    super(Set.of("books", "authors", "reviews"), scripts().add("recommendation", "").build());
  }

  @Override
  public Path getDataDirectory() {
    return DATA_DIRECTORY;
  }
}
