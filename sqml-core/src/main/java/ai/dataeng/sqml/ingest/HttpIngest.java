package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.connector.HttpSource;
import ai.dataeng.sqml.connector.JDBCSink;
import ai.dataeng.sqml.metadata.Metadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import org.postgresql.util.PGobject;

public class HttpIngest implements Runnable {

  private final HttpSource source;
  private final JDBCSink sink;
  private final Session session;
  private Metadata metadata;
  static ObjectMapper mapper = new ObjectMapper();

  interface Inter<T> {}
  class Test implements Inter<String>{}

  public HttpIngest(HttpSource source, JDBCSink sink, Session session, Metadata metadata) {
    this.source = source;
    this.sink = sink;
    this.session = session;
    this.metadata = metadata;
  }

  public void run() {
    try {
      System.out.println("Executing ingest");
      runNow();
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println(e);
      throw new RuntimeException(e);
    }
  }

  public void runNow() throws Exception {
    Connection conn = metadata.getConnection(session);
    Long time = System.currentTimeMillis();

    PreparedStatement ps = conn.prepareStatement(String.format(
        "insert into %s(json) values (?)",
        metadata.getSourceTable(source)
            .orElseThrow()
            .getName())
    );

    //todo: better dataflow semantics
    URL url = new URL(source.getSource());
    BufferedReader in = new BufferedReader(
        new InputStreamReader(url.openStream()), 1_000_000);

    while(true) {
      //Todo: make input stream not block
      if (System.currentTimeMillis() - time > 1_000) {
        int[] r = ps.executeBatch();
        System.out.println("executed: " + r.length + " time: " + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        ps.clearBatch();
      }

      //TODO: Move to json stream

      String line = in.readLine();

      JsonNode node = mapper.readTree(line);

      if (node instanceof ArrayNode) {
        System.out.println("Top level object is array, ignoring");
      } else if (node instanceof ObjectNode) {
        List<JsonNode> nodes = decorateList(node);
        PGobject jsonObject = new PGobject();
        jsonObject.setType("json");
        String value = mapper.writeValueAsString(nodes.get(0));
        jsonObject.setValue(value);

        ps.setObject(1, jsonObject);
        ps.addBatch();
      }
      //todo error queue
      //todo exception handling
    }
  }

  private List<JsonNode> decorateList(JsonNode node) {
    if (node.isArray()) {
      List<JsonNode> nodes = new ArrayList<>();
      ArrayNode arrayNode = (ArrayNode) node;
      for (Iterator<JsonNode> it2 = arrayNode.elements(); it2.hasNext(); ) {
        JsonNode arr = it2.next();
        if (arr instanceof ArrayNode) {
          throw new RuntimeException("Could not process json, nested array in root object. e.g. [[]]");
        }
        nodes.add(decorateChild(arr, ""));
      }
      return nodes;
    }

    return List.of(decorateChild(node, ""));
  }

  private JsonNode decorateChild(JsonNode node, String columnName) {
    if (node instanceof ObjectNode) {
      ObjectNode objectNode = (ObjectNode) node;

      ObjectNode result = mapper.createObjectNode();

      result.put("__id", UUID.randomUUID().toString());
      for (Iterator<Entry<String, JsonNode>> it = objectNode.fields(); it.hasNext(); ) {
        Entry<String, JsonNode> entry = it.next();
        String newColumn = columnName + "." + entry.getKey();
        //if object & has an object element, wrap in array
        JsonNode r = decorateChild(entry.getValue(), newColumn);

        if (entry.getValue().getNodeType() == JsonNodeType.OBJECT &&
            !entry.getValue().isEmpty()) {
          ArrayNode arrayNode = mapper.createArrayNode();
          arrayNode.add(decorateChild(entry.getValue(), newColumn));
          r = arrayNode;
        }

        String decoratedName = getDecoratedName(entry);
        metadata.notifyColumn(decoratedName, newColumn, r.getNodeType().toString());

        result.set(decoratedName, r);
      }
      return result;
    } else if (node instanceof ArrayNode) {
      ArrayNode arr = (ArrayNode) node;
      //verify all nested nodes are the same type
      Set<JsonNodeType> types = new HashSet<>();
      for (Iterator<JsonNode> it = arr.elements(); it.hasNext(); ) {
        JsonNode entry = it.next();
        types.add(entry.getNodeType());
      }
      if (types.size() > 1) {
        throw new RuntimeException("Json parsing error: Mismatched types in array. e.g. [{}, [], 0]");
      } else if (types.size() == 1) {
        if (types.iterator().next() == JsonNodeType.OBJECT) {
          ArrayNode arrayNode = mapper.createArrayNode();
          for (Iterator<JsonNode> it = arr.elements(); it.hasNext(); ) {
            JsonNode child = decorateChild(it.next(), columnName);
            arrayNode.add(child);
          }
          return arrayNode;
        }
      }
    }
    return node;
  }

  private String getDecoratedName(Entry<String, JsonNode> entry) {
    if (entry.getValue().getNodeType() == JsonNodeType.ARRAY) {
      ArrayNode arr = (ArrayNode) entry.getValue();
      if (arr.isEmpty()) {
        return entry.getKey() + "_" + JsonNodeType.MISSING;
      }
      return entry.getKey() + "_" + arr.iterator().next().getNodeType();
    } else {
      return entry.getKey() + "_" + entry.getValue().getNodeType();
    }
  }
}
