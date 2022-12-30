package com.datasqrl;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.name.Name;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.Column;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.json.schema.JsonSchema;
import io.vertx.json.schema.JsonSchemaOptions;
import io.vertx.json.schema.OutputUnit;
import io.vertx.json.schema.SchemaRepository;
import io.vertx.json.schema.Validator;
import io.vertx.json.schema.common.dsl.SchemaBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.junit.jupiter.api.Test;

class JsonSchemaTableSchemaFactoryTest {

  @Test
  @SneakyThrows
  public void test() {
    JsonObject schema = new JsonObject(Files.readString(
        Path.of("/Users/henneberger/sqml/myproject/openlineage/events.schema.json")));

    //todo: Add subschemas to repo
    SchemaRepository repository =
        SchemaRepository.create(new JsonSchemaOptions()
            .setBaseUri("https://openlineage.io/spec/1-0-5/")
        );

    JsonSchema facet1 = JsonSchema.of(
        new JsonObject(Files.readString(Path.of(
            "/Users/henneberger/sqml/myproject/openlineage/facets/ErrorMessageRunFacet.json"))));
    repository.dereference(facet1);

    JsonSchema jsonSchema = JsonSchema.of(schema);
    repository.dereference(jsonSchema);

//    repository.

    Validator validator = repository.validator(jsonSchema);

    ObjectMapper mapper = new ObjectMapper();
    String entry = List.of(
            Files.readString(Path.of("/Users/henneberger/sqml/myproject/openlineage/events.json"))
                .split("\n"))
        .get(0);

    Map o = mapper.readValue(entry, Map.class);

    OutputUnit result = validator.validate(o);

    System.out.println(mapper.writerWithDefaultPrettyPrinter()
        .writeValueAsString(result.toJson()));
    assertTrue(result.getValid(), result.getError());

//    JsonSchema schema1 = repository.find("https://openlineage.io/spec/1-0-5/OpenLineage.json");

//    JsonObject jsonObject = repository.resolve("https://openlineage.io/spec/1-0-5/facets/ErrorMessageRunFacet.json");
    JsonObject jsonObject2 = repository.resolve(
        "https://openlineage.io/spec/1-0-5/OpenLineage.json");
//    System.out.println(mapper.writerWithDefaultPrettyPrinter()
//        .writeValueAsString(jsonObject2));
    toUTB(repository, jsonObject2);

  }

  public class JsonSchemaModel2 {

    public class JsonSchema {

    }

    @Value
    public class CompositeSchema extends GenericSchema {
      //{ "type": ["number", "string"] }
    }


    public class IntSchema extends GenericSchema {
      //type = integer
    }

    public class NumberSchema extends GenericSchema {
      //type = number
    }
    public class StringSchema extends GenericSchema {
      //type = string
      String format;
    }

    public class BooleanSchema extends GenericSchema {
      //type = number
    }

    public class ArraySchema extends GenericSchema {

      List<JsonSchema> items;
      List<JsonSchema> prefixItems;
    }

    public class ObjectSchema extends GenericSchema {
      Map<String, JsonSchema> properties;
      Map<Pattern, JsonSchema> patternProperties;
      Set<String> required;
    }

    public class GenericSchema extends JsonSchema {
      //Will infer type from Keywords
      //keywords: const enum $ref allOf anyOf oneOf

      //if than else default dependentSchemas unevaluatedProperties
    }
  }

  private JsonSchemaModel2.JsonSchema toUTB(SchemaRepository repository, JsonObject obj) {



    if (obj.getString("type") != null) {

    } else if (obj.getString("$ref") != null) {

    }

    if (obj.getString("$ref") != null) {
      System.out.println(obj);
      JsonObject object = repository.resolve(obj.getString("$ref"));
      toUTB(repository, object);

      return null;
    }

    switch (obj.getString("type")) {
      //Can use a 'vocabulary' to map additional types
      // e.g. date-time, uuid, etc
      case "null":
      case "boolean":
      case "number":
      case "string":
      case "object":

        JsonObject props = obj.getJsonObject("properties");
        JsonArray required = obj.getJsonArray("required");
        if (props != null) {
          for (Map.Entry<String, Object> prop : props.getMap().entrySet()) {
            if (prop.getValue() instanceof JsonObject) {
              JsonObject nested = (JsonObject) prop.getValue();
              System.out.println(prop.getKey() + " : Object");
              toUTB(repository, nested);
            } else {
              System.out.println("?");

            }

          }
        }
        JsonArray any = obj.getJsonArray("anyOf");
        if (any != null) {
          for (JsonObject o : (List<JsonObject>) any.getList()) {
            toUTB(repository, o);
          }
        }
        JsonArray all = obj.getJsonArray("allOf");
        if (all != null) {
//          toUTB(all);
        }
        JsonArray one = obj.getJsonArray("oneOf");
        if (one != null) {
//          toUTB(one);
        }

        //could be: oneOf allOf anyOf not

        break;
      case "array":
        System.out.println("array:");
        JsonObject array = obj.getJsonObject("items");
        toUTB(repository, array);
//
//        for () {
//
//        }

        break;
//      case "string":
//        System.out.println(obj);
//        obj.getString("description");
//        obj.getString("format");
//        obj.getString("type");
//
//        break;
      default:
        throw new RuntimeException("unknown type");
    }

    /**
     * Schema could be graph-like.
     * Only one root though, they are all 'nested' objects
     * Able to create relationships between objects here
     *
     * What about event streams?
     */

    /**
     * We probably need more information:
     * Lets just do only tree since we'd need more information about joins.
     *
     * We unfortunately need predictable tree structures
     *
     * Take schema and walk, if we get to a def, expand it and keep a list of visited so we don't repeat
     *
     * If stop stream is oneOf, allow splitting on an identifier
     *
     */
    /**
     * {
     *   "splitOnField": "type"
     * }
     *
     * All the features i'd want this to have:
     * 1. Can create multiple Tables for a ns.
     *
     * Create a github issue:
     * 1. Polymorphism?
     * 2. Recursion
     * 3.
     *
     */

    UniversalTable table;

    //parse(schema);
    obj.getJsonObject("$defs");

    System.out.println();

    return null;
  }

}