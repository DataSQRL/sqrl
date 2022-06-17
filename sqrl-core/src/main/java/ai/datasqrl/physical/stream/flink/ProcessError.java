package ai.datasqrl.physical.stream.flink;

import ai.datasqrl.config.error.ErrorCollector;
import lombok.Value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Value
public class ProcessError<Input> implements Serializable {

    private List<String> errors;
    private Input input;

    public static <Input> ProcessError<Input> of(ErrorCollector errors, Input input) {
        List<String> errorMsgs = new ArrayList<>();
        errors.forEach(e -> errorMsgs.add(e.toString()));
        return new ProcessError(errorMsgs, input);
    }

}
