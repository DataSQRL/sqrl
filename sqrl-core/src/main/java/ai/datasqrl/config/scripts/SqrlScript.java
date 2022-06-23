package ai.datasqrl.config.scripts;

import ai.datasqrl.config.constraints.OptionalMinString;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.util.ConfigurationUtil;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.schema.input.external.SchemaDefinition;
import java.io.Serializable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@Value
public class SqrlScript implements Serializable {

  private final Name name;
  private final String filename;
  private final String content;
  private final SchemaDefinition schema;
  private boolean main;


  @Builder
  @Getter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config {

    @NonNull
    @NotNull
    @Size(min = 3, max = 128)
    private String name;
    private String filename;
    @NonNull
    @NotNull
    @Size(min = 10)
    private String content;

    @OptionalMinString
    private String inputSchema;
    @Builder.Default
    private boolean main = false;

    SqrlScript initialize(ErrorCollector errors, NameCanonicalizer canonicalizer) {
      if (!ConfigurationUtil.javaxValidate(this, errors)) {
        return null;
      }
      errors = errors.resolve(name);
      SchemaDefinition schema;
      try {
        schema = parseSchema(inputSchema);
      } catch (JsonProcessingException e) {
        errors.fatal("Parsing error for schemaYaml: [%s]", e);
        return null;
      }
      return new SqrlScript(canonicalizer.name(name),
          StringUtils.isNotEmpty(filename) ? filename : name,
          content, schema, main);
    }

    public static SchemaDefinition parseSchema(String inputSchema) throws JsonProcessingException {
      if (StringUtils.isEmpty(inputSchema)) {
        return SchemaDefinition.empty();
      } else {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        SchemaDefinition importSchema = mapper.readValue(inputSchema,
            SchemaDefinition.class);
        return importSchema;
      }
    }

  }


}
