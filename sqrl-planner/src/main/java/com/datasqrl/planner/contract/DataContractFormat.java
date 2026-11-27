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
package com.datasqrl.planner.contract;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * A format of data contracts, such as ODCS. The format reads the contract files of a project,
 * compares what a contract says about a table with the table, and writes the contracts of a
 * deployment. Which file belongs to which script, and which tables get a contract, does not depend
 * on the format.
 *
 * <p>The formats are found through {@link ServiceLoaderDiscovery}.
 */
public interface DataContractFormat {

  /** What a contract file says about one table. */
  interface Entry {

    /** The name of the table that the entry describes */
    String name();

    /** Where the entry starts in its file */
    FileLocation location();
  }

  /**
   * The entries of a contract file.
   *
   * @param entries the entries, whose names differ in more than their case
   * @param location where the entries start in the file, for errors about the entries as a whole
   * @param declaresTags whether an entry declares tags or a classification
   */
  record Entries(List<? extends Entry> entries, FileLocation location, boolean declaresTags) {}

  /**
   * A difference between an entry and its table.
   *
   * @param location where the entry says what differs
   */
  record Difference(FileLocation location, String message) {}

  /**
   * What a contract of the deployment says about one table. Two generated entries are equal if they
   * say the same.
   */
  interface GeneratedEntry {

    String name();
  }

  /** The extension of a contract file, without the leading dot, e.g. {@code odcs.yaml} */
  String getFileExtension();

  /** The extensions of files that look like a contract of this format but are not read */
  List<String> getUnsupportedExtensions();

  /** What the format calls an entry in messages, e.g. {@code schema object} */
  String getEntryLabel();

  /**
   * Reads a contract file. Every error of the file is reported, at its line where it has one.
   *
   * @return the entries of the file, or empty if it has errors
   */
  Optional<Entries> read(String content, ErrorCollector errors);

  /**
   * @param entry an entry of a file that this format read
   * @return every difference between the entry and the table
   */
  List<Difference> compare(Entry entry, ContractSubject table, TagDefinitions tagDefinitions);

  /**
   * @param provided the entry that a contract file of this format provides for the table, if any
   * @return what a contract of the deployment says about the table
   */
  GeneratedEntry describe(
      ContractSubject table, Optional<Entry> provided, TagDefinitions tagDefinitions);

  /**
   * @param name the name of the contract, which is its file name without the extension
   * @return the content of the contract file
   */
  String write(String name, List<GeneratedEntry> entries);

  /** The name of the contract file of a script, or of a contract of the deployment. */
  default String fileName(String name) {
    return name + "." + getFileExtension();
  }

  /**
   * Whether a file has the extension of this format, in any case. A file that writes the extension
   * in another case is not read, but it must not be ignored silently.
   */
  default boolean hasFileExtension(String fileName) {
    return fileName.toLowerCase(Locale.ROOT).endsWith("." + getFileExtension());
  }

  /** All formats, in the order of their file extensions. */
  static List<DataContractFormat> all() {
    return ServiceLoaderDiscovery.getAll(DataContractFormat.class).stream()
        .sorted(Comparator.comparing(DataContractFormat::getFileExtension))
        .toList();
  }
}
