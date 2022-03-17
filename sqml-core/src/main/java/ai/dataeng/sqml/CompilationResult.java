package ai.dataeng.sqml;

import ai.dataeng.sqml.config.scripts.ScriptBundle;
import ai.dataeng.sqml.config.scripts.SqrlScript;
import ai.dataeng.sqml.tree.name.Name;
import lombok.Builder;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Value
@Builder
public class CompilationResult {

    private final int compileTime;
    private final Status status;
    private final List<Compilation> compilations;

    public enum Status {
        success, failed;
    }

    public enum Type {
        script, query;
    }

    @Value
    @Builder
    public static class Compilation {
        private final String name;
        private final String filename;
        private final String preschema;
        private final List<Message> messages;
        private final Status status;
        private final Type type;
    }

    @Value
    public static class Message {

        private final Type type;
        private final String location;
        private final String message;

        public enum Type {
            error, warning, information;
        }

    }

    public static CompilationResult generateDefault(ScriptBundle bundle, long compileTimeMs) {
        assert compileTimeMs < Integer.MAX_VALUE;
        List<Compilation> compilations = new ArrayList<>();
        for (SqrlScript script : bundle.getScripts().values()) {

            compilations.add(Compilation.builder()
                            .name(script.getName().getDisplay())
                            .filename(script.getFilename())
                            .preschema("")
                            .messages(Collections.EMPTY_LIST)
                            .status(Status.success)
                            .type(Type.script)
                            .build());
        }
        return CompilationResult.builder().status(Status.success).compileTime((int)compileTimeMs)
                .compilations(compilations).build();

    }

}
