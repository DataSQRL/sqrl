package ai.dataeng.sqml.io.sources.util;

import lombok.*;

import java.io.Serializable;
import java.time.Instant;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class TimeAnnotatedRecord<R> implements Serializable {

    @NonNull R record;
    Instant sourceTime;

    public boolean hasTime() {
        return sourceTime!=null;
    }

}
