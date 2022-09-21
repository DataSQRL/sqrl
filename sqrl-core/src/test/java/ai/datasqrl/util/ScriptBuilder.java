package ai.datasqrl.util;

import lombok.Value;

@Value
public class ScriptBuilder {

    private final StringBuilder s = new StringBuilder();

    public ScriptBuilder add(String statement) {
        s.append(statement);
        if (!statement.endsWith(";\n")) {
            if (statement.endsWith(";")) {
                s.append("\n");
            } else {
                s.append(";\n");
            }
        }
        return this;
    }

    public ScriptBuilder append(String statement) {
        return add(statement);
    }

    public String getScript() {
        return toString();
    }

    @Override
    public String toString() {
        return s.toString();
    }

    public static String of(String... statements) {
        ScriptBuilder s = new ScriptBuilder();
        for (String statement : statements) {
            s.add(statement);
        }
        return s.toString();
    }

}
