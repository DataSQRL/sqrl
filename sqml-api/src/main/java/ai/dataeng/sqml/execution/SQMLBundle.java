package ai.dataeng.sqml.execution;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * An {@link SQMLBundle} contains the main SQML script that defines the dataset to be exposed as an API as well
 * as all supporting SQML scripts that are imported (directly or indirectly) by the main script.
 *
 * In addition, the bundle may include an optional schema file that defines the schema of the input data, API, and can
 * provide additional hints that guide the optimizer on how to generate the denormalizations.
 */
public class SQMLBundle {

    private final Map<String, String> scriptsByname;
    private final String mainScriptName;
    //TODO: Add schema and hints


    private SQMLBundle(Map<String, String> scriptsByname, String mainScriptName) {
        this.scriptsByname = scriptsByname;
        this.mainScriptName = mainScriptName;
    }




    public static class Builder {

        private final Map<String, String> scriptsByname = new HashMap<>();
        private String mainScriptName;

        public void addScript(String name, String content) {
            checkScriptName(name);
            Preconditions.checkArgument(!scriptsByname.containsKey(name));
            scriptsByname.put(name,content);
        }

        public void setMainScript(String name) {
            checkScriptName(name);
            Preconditions.checkArgument(mainScriptName == null);
            mainScriptName = name;
        }

        public void checkScriptName(String name) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(name));
        }

        public SQMLBundle build() {
            return new SQMLBundle(scriptsByname,mainScriptName);
        }


    }

}
