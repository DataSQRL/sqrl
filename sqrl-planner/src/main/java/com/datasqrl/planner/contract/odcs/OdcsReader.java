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
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.contract.DataContractFormat.Entries;
import com.datasqrl.planner.contract.DataContractFormat.Entry;
import com.datasqrl.util.JsonSchemaValidator;
import com.datasqrl.util.JsonSchemaValidator.Violation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Reads an ODCS contract file: its YAML, its version, its validity against the JSON schema of ODCS,
 * and its schema objects. Every error is reported at its line.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class OdcsReader {

  private static final String STRING_TYPE = "string";

  /**
   * A schema object of a contract file.
   *
   * @param pointer the JSON pointer of the schema object in its document
   * @param schemaObject the schema object, with all fields that the file has
   */
  record OdcsEntry(String name, String pointer, JsonNode schemaObject, OdcsDocument document)
      implements Entry {

    @Override
    public FileLocation location() {
      return document.locate(pointer);
    }
  }

  /**
   * @param fileErrors reports errors in the file
   * @return the schema objects of the file, or empty if it has errors
   */
  static Optional<Entries> read(String content, ErrorCollector fileErrors) {
    try {
      return parse(content, fileErrors);
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

  private static Optional<Entries> parse(String content, ErrorCollector fileErrors)
      throws IOException {

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

    var document = OdcsDocument.read(content);
    var validVersion = checkVersion(document, fileErrors);
    var valid = validateSchema(document, fileErrors, validVersion);
    var entries = new ArrayList<OdcsEntry>();
    valid &= readEntries(document, fileErrors, entries);

    if (!valid || !validVersion) {
      return Optional.empty();
    }

    return Optional.of(
        new Entries(
            entries,
            document.locate("/" + OdcsConstants.SCHEMA_FIELD),
            elements(document.root().path(OdcsConstants.SCHEMA_FIELD))
                .anyMatch(OdcsReader::declaresTags)));
  }

  private static boolean checkVersion(OdcsDocument document, ErrorCollector fileErrors) {
    var apiVersion = document.root().path(OdcsConstants.API_VERSION_FIELD).asText("").trim();
    if (apiVersion.startsWith(OdcsConstants.SUPPORTED_VERSION_PREFIX)) {
      return true;
    }

    fileErrors
        .atFile(document.locate("/" + OdcsConstants.API_VERSION_FIELD))
        .fatal(
            apiVersion.isEmpty()
                ? "The contract has no apiVersion: set it to %2$s"
                : "apiVersion [%s] is not supported: convert the contract to ODCS %s",
            apiVersion,
            OdcsConstants.API_VERSION);

    return false;
  }

  /**
   * Reads the schema objects, whose names must differ in more than their case, since table names
   * are matched ignoring case.
   *
   * @return false if two schema objects have the same name
   */
  private static boolean readEntries(
      OdcsDocument document, ErrorCollector fileErrors, List<OdcsEntry> entries) {
    var schema = document.root().path(OdcsConstants.SCHEMA_FIELD);
    var names = new HashMap<String, String>();
    var valid = true;
    // A schema that is not a list is reported by the schema validation
    for (var i = 0; schema.isArray() && i < schema.size(); i++) {
      var name = schema.get(i).path(OdcsConstants.NAME_FIELD).asText(null);
      if (name == null) {
        continue; // reported by the schema validation
      }
      var pointer = "/" + OdcsConstants.SCHEMA_FIELD + "/" + i;
      var existing = names.putIfAbsent(name.toLowerCase(Locale.ROOT), name);
      if (existing == null) {
        entries.add(new OdcsEntry(name, pointer, schema.get(i), document));
      } else {
        fileErrors
            .atFile(document.locate(pointer))
            .fatal(
                existing.equals(name)
                    ? "Schema object [%s] is defined more than once"
                    : "Schema objects [%2$s] and [%1$s] differ only in case. Table names are"
                        + " matched ignoring case",
                name,
                existing);
        valid = false;
      }
    }
    return valid;
  }

  private static boolean validateSchema(
      OdcsDocument document, ErrorCollector fileErrors, boolean validVersion) {
    List<Violation> violations;
    Set<String> odcsFields;
    try {
      violations =
          JsonSchemaValidator.validate(document.root(), OdcsConstants.JSON_SCHEMA_RESOURCE);
      odcsFields = JsonSchemaValidator.propertyNames(OdcsConstants.JSON_SCHEMA_RESOURCE);
    } catch (IOException e) {
      throw new IllegalStateException("Could not load the ODCS JSON schema", e);
    }

    var valid = true;
    for (var violation : withoutCollateral(violations, odcsFields)) {
      if (!validVersion
          && violation.instancePointer().equals("/" + OdcsConstants.API_VERSION_FIELD)) {
        continue; // already reported
      }
      report(violation, document, fileErrors);
      valid = false;
    }
    return valid;
  }

  /** Reports a violation at the value it is about, and names the field of that value. */
  private static void report(
      Violation violation, OdcsDocument document, ErrorCollector fileErrors) {
    var pointer = violation.instancePointer();
    var message = violation.message();
    if (violation.isUnevaluatedProperty()) {
      // The violation is at the object, and it is about one of its fields
      pointer += "/" + violation.property().replace("~", "~0").replace("/", "~1");
      message = "not a field that ODCS defines for this element";
    } else {
      var value = document.root().at(pointer);
      if (violation.isNotOfType(STRING_TYPE) && (value.isNumber() || value.isBoolean())) {
        message += ". Quote the value to make it a string";
      }
    }

    var field = pointer.substring(pointer.lastIndexOf('/') + 1);
    var subject = field.isEmpty() || field.chars().allMatch(Character::isDigit) ? "" : field + ": ";
    fileErrors.atFile(document.locate(pointer)).fatal("%s%s", subject, message);
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

  /**
   * Whether a schema object or a property declares tags or a classification, on itself or on what
   * is nested in it: its properties, the items of an array, and the key and value of a map. These
   * are the places whose tags are compared with the table.
   */
  private static boolean declaresTags(JsonNode element) {
    if (element.isMissingNode()) {
      return false;
    }
    if (!element.path(OdcsConstants.TAGS_FIELD).isEmpty()
        || element.hasNonNull(OdcsConstants.CLASSIFICATION_FIELD)) {
      return true;
    }
    var map = element.path("map");
    return Stream.concat(
            elements(element.path(OdcsConstants.PROPERTIES_FIELD)),
            Stream.of(element.path("items"), map.path("key"), map.path("value")))
        .anyMatch(OdcsReader::declaresTags);
  }

  private static Stream<JsonNode> elements(JsonNode array) {
    return StreamSupport.stream(array.spliterator(), false);
  }
}
