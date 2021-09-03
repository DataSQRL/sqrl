package ai.dataeng.sqml.ingest.schema.external.validation;

import ai.dataeng.sqml.ingest.schema.name.NamePath;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

public class ValidationResult {

    public List<Message> errors;
    public List<Message> warnings;

    public ValidationResult() {
        errors = new ArrayList<>();
        warnings = new ArrayList<>();
    }

    public void addError(NamePath location, String message, Object... args) {
        errors.add(new Message(location,message,args));
    }

    public void addWarning(NamePath location, String message, Object... args) {
        warnings.add(new Message(location,message,args));
    }

    public boolean isError() {
        return !errors.isEmpty();
    }

    @Value
    public static class Message {

        public NamePath location;
        public String message;

        public Message(NamePath location, String message, Object... args) {
            this.location = location;
            this.message = String.format(message, args);
        }

    }

}
