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

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.planner.contract.DataContractFile.SchemaObject;
import com.datasqrl.util.JsonSchemaValidator;
import com.datasqrl.util.JsonSchemaValidator.Violation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.Strings;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Loads the data contract files that sit next to the files that define tables: {@code
 * mytables.sqrl} pairs with {@code mytables.odcs.yaml}. Every file is loaded once per compilation.
 *
 * <p>All errors of a contract file are recorded without aborting, so that the errors of all files
 * are reported together. A file with errors attaches nothing.
 */
@Component
@Lazy
public class DataContractLoader {

  private static final String STRING_TYPE = "string";

  private final ErrorCollector errors;

  /** The sources by the normalized path of their defining file */
  private final Map<Path, ContractSource> sources = new HashMap<>();

  /** The contract files that were found, by path; empty for a file with errors */
  private final Map<Path, Optional<DataContractFile>> contractFiles = new LinkedHashMap<>();

  public DataContractLoader(ErrorCollector errors) {
    this.errors = errors.abortOnFatal(false);
  }

  /**
   * @param definingFile a file that defines tables, a script or a {@code .table.sql} file
   * @return the source of the contract file next to it
   */
  public ContractSource forFile(Path definingFile) {
    var file = definingFile.toAbsolutePath().normalize();
    return sources.computeIfAbsent(
        file,
        f ->
            new FileContractSource(
                this,
                f,
                stemOf(f.getFileName().toString())
                    .flatMap(stem -> findContractFile(f, stem))
                    .flatMap(this::loadOnce),
                errors));
  }

  public ContractSource forFile(Optional<Path> definingFile) {
    return definingFile.map(this::forFile).orElse(ContractSource.NONE);
  }

  /** The valid contract files that were loaded, in the order they were first used. */
  public List<DataContractFile> getLoadedFiles() {
    return contractFiles.values().stream().flatMap(Optional::stream).toList();
  }

  /** The paths of all contract files that were found next to a planned file, valid or not. */
  public Set<Path> getFoundFiles() {
    return contractFiles.keySet();
  }

  /**
   * @return the name of a defining file without its extension, which is the name of its contract
   *     file without {@code .odcs.yaml}; empty if the file cannot define tables
   */
  public static Optional<String> stemOf(String fileName) {
    return Stream.of(ModuleLoaderImpl.TABLE_FILE_SUFFIX, ModuleLoaderImpl.SQRL_FILE_EXTENSION)
        .filter(fileName::endsWith)
        .map(suffix -> Strings.CS.removeEnd(fileName, suffix))
        .findFirst();
  }

  /**
   * Finds the contract file by comparing the names in the folder, so that the result does not
   * depend on whether the file system is case-sensitive.
   */
  private Optional<Path> findContractFile(Path definingFile, String stem) {
    var folder = definingFile.getParent();
    var expected = OdcsConstants.fileName(stem);
    List<String> names;
    try (var files = Files.list(folder)) {
      names = files.map(path -> path.getFileName().toString()).toList();
    } catch (IOException e) {
      return Optional.empty();
    }

    if (names.contains(expected)) {
      return Optional.of(folder.resolve(expected));
    }
    names.stream()
        .filter(name -> name.equalsIgnoreCase(expected))
        .forEach(
            name ->
                // Such a pair works on a case-insensitive file system and not on others
                errors.fatal(
                    ErrorCode.INVALID_DATA_CONTRACT,
                    "Contract file [%s] differs from the name of [%s] only in case and is not"
                        + " read. Rename it to [%s]",
                    name,
                    definingFile.getFileName(),
                    expected));
    return Optional.empty();
  }

  /** Two files that define tables can share a contract file: a script and a table file. */
  private Optional<DataContractFile> loadOnce(Path contractFile) {
    var loaded = contractFiles.get(contractFile);
    if (loaded == null) {
      loaded = load(contractFile);
      contractFiles.put(contractFile, loaded);
    }
    return loaded;
  }

  /** Reports every error of the file, and then once that the file is not applied. */
  private Optional<DataContractFile> load(Path path) {
    var file = read(path);
    if (file.isEmpty()) {
      errors.fatal(
          ErrorCode.INVALID_DATA_CONTRACT,
          "Contract file [%s] is not a valid data contract and is not applied",
          path.getFileName());
    }
    return file;
  }

  private Optional<DataContractFile> read(Path path) {
    String content;
    try {
      content = Files.readString(path);
    } catch (IOException e) {
      errors.fatal("Could not read contract file [%s]: %s", path, e);
      return Optional.empty();
    }

    var fileErrors = errors.withScript(path, content);
    try {
      return parse(path, content, fileErrors);
    } catch (JsonProcessingException e) {
      var location = e.getLocation();
      (location == null
              ? fileErrors
              : fileErrors.atFile(new FileLocation(location.getLineNr(), location.getColumnNr())))
          .fatal("Invalid YAML: %s", e.getOriginalMessage());
    } catch (IOException e) {
      fileErrors.fatal("Invalid YAML: %s", e.getMessage());
    }
    return Optional.empty();
  }

  private static Optional<DataContractFile> parse(
      Path path, String content, ErrorCollector fileErrors) throws IOException {
    var documents = OdcsYaml.readDocuments(content);
    if (documents.size() > 1) {
      // A YAML parser reads the first document and drops the others
      fileErrors.fatal(
          "The contract file has %s YAML documents. A contract file holds one document",
          documents.size());
      return Optional.empty();
    }
    if (documents.isEmpty() || !documents.get(0).isObject()) {
      fileErrors.fatal(
          documents.isEmpty()
              ? "The contract file is empty"
              : "The contract file does not hold a data contract: it must be a YAML mapping with"
                  + " the fields of ODCS, such as apiVersion and schema");
      return Optional.empty();
    }

    var root = documents.get(0);
    var locations = YamlLocations.of(content);
    var version = parseVersion(root, locations, fileErrors);
    var valid = validateSchema(root, locations, fileErrors, version.isPresent());
    var schemaObjects = new LinkedHashMap<String, SchemaObject>();
    valid &= indexSchemaObjects(root, locations, fileErrors, schemaObjects);

    return valid && version.isPresent()
        ? Optional.of(
            new DataContractFile(path, content, root, version.get(), locations, schemaObjects))
        : Optional.empty();
  }

  private static Optional<OdcsVersion> parseVersion(
      JsonNode root, YamlLocations locations, ErrorCollector fileErrors) {
    try {
      return Optional.of(OdcsVersion.parse(root.path("apiVersion").asText(null)));
    } catch (IllegalArgumentException e) {
      fileErrors.atFile(locations.locate("/apiVersion")).fatal("%s", e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * Indexes the schema objects by their name in lower case, since table names are matched ignoring
   * case.
   *
   * @return false if two schema objects have the same name
   */
  private static boolean indexSchemaObjects(
      JsonNode root,
      YamlLocations locations,
      ErrorCollector fileErrors,
      Map<String, SchemaObject> schemaObjects) {
    var schema = root.path("schema");
    var valid = true;
    // A schema that is not a list is reported by the schema validation
    for (var i = 0; schema.isArray() && i < schema.size(); i++) {
      var name = schema.get(i).path("name").asText(null);
      if (name == null) {
        continue; // reported by the schema validation
      }
      var pointer = "/schema/" + i;
      var existing =
          schemaObjects.putIfAbsent(
              DataContractFile.key(name), new SchemaObject(name, pointer, schema.get(i)));
      if (existing != null) {
        fileErrors
            .atFile(locations.locate(pointer))
            .fatal(
                existing.name().equals(name)
                    ? "Schema object [%s] is defined more than once"
                    : "Schema objects [%2$s] and [%1$s] differ only in case. Table names are"
                        + " matched ignoring case",
                name,
                existing.name());
        valid = false;
      }
    }
    return valid;
  }

  private static boolean validateSchema(
      JsonNode root, YamlLocations locations, ErrorCollector fileErrors, boolean validVersion) {
    List<Violation> violations;
    Set<String> odcsFields;
    try {
      violations = JsonSchemaValidator.validate(root, OdcsConstants.JSON_SCHEMA_RESOURCE);
      odcsFields = JsonSchemaValidator.propertyNames(OdcsConstants.JSON_SCHEMA_RESOURCE);
    } catch (IOException e) {
      throw new IllegalStateException("Could not load the ODCS JSON schema", e);
    }

    var valid = true;
    for (var violation : withoutCollateral(violations, odcsFields)) {
      if (!validVersion && violation.instancePointer().equals("/apiVersion")) {
        continue; // already reported
      }
      report(violation, root, locations, fileErrors);
      valid = false;
    }
    return valid;
  }

  /** Reports a violation at the value it is about, and names the field of that value. */
  private static void report(
      Violation violation, JsonNode root, YamlLocations locations, ErrorCollector fileErrors) {
    var pointer = violation.instancePointer();
    var message = violation.message();
    if (violation.isUnevaluatedProperty()) {
      // The violation is at the object, and it is about one of its fields
      pointer += "/" + violation.property().replace("~", "~0").replace("/", "~1");
      message = "not a field that ODCS defines for this element";
    } else {
      var value = root.at(pointer);
      if (violation.isNotOfType(STRING_TYPE) && (value.isNumber() || value.isBoolean())) {
        message += ". Quote the value to make it a string";
      }
    }

    var field = pointer.substring(pointer.lastIndexOf('/') + 1);
    var subject = field.isEmpty() || field.chars().allMatch(Character::isDigit) ? "" : field + ": ";
    fileErrors.atFile(locations.locate(pointer)).fatal("%s%s", subject, message);
  }

  /**
   * A field that ODCS does not define, or an invalid value, makes the validator report every field
   * of the object as not evaluated. Of those, only the fields that ODCS does not define are
   * reported, unless nothing else explains why the object is invalid.
   */
  private static List<Violation> withoutCollateral(
      List<Violation> violations, Set<String> odcsFields) {
    var reported =
        violations.stream()
            .filter(
                violation ->
                    !violation.isUnevaluatedProperty()
                        || !odcsFields.contains(violation.property()))
            .distinct()
            .toList();
    var unexplained =
        violations.stream()
            .filter(
                violation ->
                    violation.isUnevaluatedProperty() && odcsFields.contains(violation.property()))
            .filter(
                violation ->
                    reported.stream()
                        .noneMatch(
                            other ->
                                isAtOrBelow(other.instancePointer(), violation.instancePointer())))
            .distinct();
    return Stream.concat(reported.stream(), unexplained).toList();
  }

  private static boolean isAtOrBelow(String pointer, String parent) {
    return pointer.equals(parent) || pointer.startsWith(parent + "/");
  }
}
