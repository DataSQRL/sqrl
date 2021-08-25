package ai.dataeng.sqml.execution;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * An {@link Bundle} contains the main SQML script that defines the dataset to be exposed as an API as well
 * as all supporting SQML scripts that are imported (directly or indirectly) by the main script.
 *
 * In addition, the bundle may include an optional schema file that defines the schema of the input data, API, and can
 * provide additional hints that guide the optimizer on how to generate the denormalizations.
 *
 * Production {@link Bundle} must also contain the queries and subscriptions that get deployed in the API.
 */
@Getter
public class Bundle {
    private final Map<String, String> scriptsByname;
    private final String mainScriptName;
    private final Path path;
    //TODO: Add schema and hints

    public static Bundle.Builder bundle() {
        return new Bundle.Builder();
    }

    private Bundle(Map<String, String> scriptsByname, String mainScriptName, Path path) {
        this.scriptsByname = scriptsByname;
        this.mainScriptName = mainScriptName;
        this.path = path;
    }

    public static class Builder {

        private final Map<String, String> scriptsByname = new HashMap<>();
        private String mainScriptName;
        private Path path;

        public Builder addScript(String name, String content) {
            checkScriptName(name);
            Preconditions.checkArgument(!scriptsByname.containsKey(name));
            scriptsByname.put(name,content);
            return this;
        }

        public Builder setMainScript(String name) {
            checkScriptName(name);
            Preconditions.checkArgument(mainScriptName == null);
            mainScriptName = name;
            return this;
        }

        public Builder setMainScript(String name, String content) {
            addScript(name, content);
            setMainScript(name);
            return this;
        }

        public Builder setPath(Path path) {
            this.path = path;
            return this;
        }

        private void checkScriptName(String name) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(name));
        }

        public Bundle build() {
            return new Bundle(scriptsByname, mainScriptName, path);
        }
    }
}
