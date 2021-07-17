package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.model.Model;
import ai.dataeng.sqml.sql.tree.Import;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil.Test;

public class ImportResolver {

  public void resolve(Import node, Model analysis) {
//    QualifiedName path = node.getQualifiedName();
    //Todo: Expand out import resolution
    ObjectMapper mapper = new YAMLMapper();
    try {
      ImportSchema schema = mapper.readValue(new File("/Users/henneberger/Projects/sqml-official/sqml-examples/ecommerce/ecommerce-data/schema.yml"), ImportSchema.class);
      System.out.println(schema);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ImportSchema {
    public String version;
    public List<ImportTable> tables;
  }
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ImportTable {
    public String name;
    public String description;
    public ImportSource source;
    public List<ImportColumn> columns;
  }
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ImportSource {
    public String type;
    public String name;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ImportColumn {
    public String name;
    public String description;
    public List<String> tests;
    public List<ImportColumn> columns;
  }
}
