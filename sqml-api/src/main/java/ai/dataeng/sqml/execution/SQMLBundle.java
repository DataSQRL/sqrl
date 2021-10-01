package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.ingest.schema.external.SchemaDefinition;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.util.*;

/**
 * An {@link SQMLBundle} contains the main SQML script that defines the dataset to be exposed as an API as well
 * as all supporting SQML scripts that are imported (directly or indirectly) by the main script.
 *
 * In addition, the bundle may include an optional schema file that defines the schema of the input data, API, and can
 * provide additional hints that guide the optimizer on how to generate the denormalizations.
 *
 * Production {@link SQMLBundle} must also contain the queries and subscriptions that get deployed in the API.
 */
@Getter
public class SQMLBundle {

    private static final NameCanonicalizer CANONICALIZER = NameCanonicalizer.SYSTEM;

    private final List<SQMLScript> scripts;
    private final Path path;
    //TODO: Add schema and hints

    public static SQMLBundle.Builder bundle() {
        return new SQMLBundle.Builder();
    }

    private SQMLBundle(List<SQMLScript> scripts, Path path) {
        this.scripts = scripts;
        this.path = path;
    }

    public SQMLScript getMainScript() {
        return scripts.stream().filter(SQMLScript::isMain).findFirst().get();
    }

    public static class Builder {

        private final List<SQMLScript> scripts = new ArrayList<>();
        private Path path;

        public Builder addScript(String name, String script) {

            return this;
        }

        public Script createScript() {
            return new Script();
        }

        public Builder addMainScript(String name, String script) {
            createScript().setName(name).setScript(script).asMain().add();
            return this;
        }

        public Builder setPath(Path path) {
            this.path = path;
            return this;
        }


        public SQMLBundle build() {
            Preconditions.checkArgument(scripts.stream().anyMatch(SQMLScript::isMain),"Bundle does not have a main script");
            return new SQMLBundle(scripts, path);
        }

        public class Script {

            private Name name;
            private String scriptContent;
            private String importSchema;
            private boolean isMain = false;

            private Script() {
            }

            public Script setName(@NonNull String name) {
                return setName(Name.of(name,CANONICALIZER));
            }

            public Script setName(@NonNull Name name) {
                this.name = name;
                return this;
            }

            public Script setScript(@NonNull String script) {
                Preconditions.checkArgument(StringUtils.isNotEmpty(script),"Script cannot be empty");
                this.scriptContent = script;
                return this;
            }

            public Script setScript(@NonNull Path scriptFile) throws IOException {
                return setScript(Files.readString(scriptFile));
            }

            public Script setImportSchema(@NonNull String schemaYAML) {
                Preconditions.checkArgument(StringUtils.isNotEmpty(schemaYAML),"Import schema cannot be empty");
                this.importSchema = schemaYAML;
                return this;
            }

            public Script setImportSchema(@NonNull Path schemaYAML) throws IOException {
                return setImportSchema(Files.readString(schemaYAML));
            }

            public Script asMain() {
                this.isMain = true;
                return this;
            }

            public Builder add() {
                Preconditions.checkNotNull(name,"Need to specify a name for script");
                Preconditions.checkArgument(StringUtils.isNotEmpty(scriptContent),"Need to specify script content");
                Preconditions.checkArgument(Builder.this.scripts.stream().noneMatch(s -> s.name.equals(name)),
                        "Script with name [%s] has already been added", name);
                Preconditions.checkArgument(!isMain || Builder.this.scripts.stream().noneMatch(SQMLScript::isMain),
                        "Main script has already been added");
                SQMLScript script = new SQMLScript(name,scriptContent,Optional.ofNullable(importSchema),isMain);
                Builder.this.scripts.add(script);
                return Builder.this;
            }

        }
    }




    @Value
    public static class SQMLScript {

        @NonNull
        private final Name name;
        @NonNull
        private final String scriptContent;
        @NonNull
        private final Optional<String> importSchemaYAML;
        private final boolean isMain;


        public SchemaDefinition parseSchema() throws JsonProcessingException {
            if (importSchemaYAML.isEmpty()) return SchemaDefinition.empty();
            else {
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                SchemaDefinition importSchema = mapper.readValue(importSchemaYAML.get(),
                        SchemaDefinition.class);
                return importSchema;
            }
        }

    }
}
