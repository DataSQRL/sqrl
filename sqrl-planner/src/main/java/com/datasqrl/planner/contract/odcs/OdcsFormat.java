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
package com.datasqrl.planner.contract.odcs;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.contract.ContractSubject;
import com.datasqrl.planner.contract.DataContractFile;
import com.datasqrl.planner.contract.DataContractFormat;
import com.datasqrl.planner.contract.odcs.OdcsReader.OdcsEntry;
import com.datasqrl.planner.contract.odcs.model.OdcsContract;
import com.datasqrl.planner.tags.TagDefinitions;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Data contracts in the <a href="https://bitol-io.github.io/open-data-contract-standard/">Open Data
 * Contract Standard</a> v3.2. The entry of a table is a schema object of the contract.
 */
@AutoService(DataContractFormat.class)
public class OdcsFormat implements DataContractFormat {

  /**
   * A schema object of a contract of the deployment.
   *
   * @param schemaObject the schema object as JSON: a generated one, or one that a contract file
   *     provides, which keeps all fields that the file has
   */
  record OdcsGeneratedEntry(JsonNode schemaObject) implements GeneratedEntry {

    @Override
    public String name() {
      return schemaObject.path(OdcsConstants.NAME_FIELD).asText("");
    }
  }

  @Override
  public String getFileExtension() {
    return OdcsConstants.FILE_EXTENSION;
  }

  @Override
  public List<String> getUnsupportedExtensions() {
    return OdcsConstants.UNSUPPORTED_FILE_EXTENSIONS;
  }

  @Override
  public String getEntryLabel() {
    return "schema object";
  }

  @Override
  public Optional<DataContractFile> read(Path path, String content, ErrorCollector errors) {
    return OdcsReader.read(path, content, this, errors);
  }

  @Override
  public List<Difference> compare(
      Entry entry, ContractSubject table, TagDefinitions tagDefinitions) {
    var schemaObject = (OdcsEntry) entry;
    return new OdcsCompatibilityChecker(tagDefinitions)
            .compare(
                OdcsYaml.toSchemaObject(schemaObject.schemaObject()),
                schemaObject.pointer(),
                new OdcsGenerator(tagDefinitions).generate(table),
                table)
            .stream()
            .map(
                difference ->
                    new Difference(
                        schemaObject.document().locate(difference.pointer()), difference.message()))
            .toList();
  }

  @Override
  public GeneratedEntry describe(
      ContractSubject table, Optional<Entry> provided, TagDefinitions tagDefinitions) {
    return new OdcsGeneratedEntry(
        provided
            .map(entry -> ((OdcsEntry) entry).schemaObject())
            .orElseGet(() -> OdcsYaml.toJson(new OdcsGenerator(tagDefinitions).generate(table))));
  }

  @Override
  public String write(String name, List<GeneratedEntry> entries) {
    var contract =
        new OdcsContract(
            OdcsVersion.API_VERSION,
            OdcsConstants.KIND,
            name,
            name,
            OdcsConstants.GENERATED_VERSION,
            entries.stream().map(entry -> ((OdcsGeneratedEntry) entry).schemaObject()).toList());
    try {
      return OdcsYaml.write(contract);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not write the data contract " + name, e);
    }
  }
}
