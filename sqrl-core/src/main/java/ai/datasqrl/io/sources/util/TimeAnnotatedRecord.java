package ai.datasqrl.io.sources.util;

import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@ToString
@EqualsAndHashCode
public class TimeAnnotatedRecord<R> implements Serializable {

  @NonNull R record;
  Instant sourceTime;

  public TimeAnnotatedRecord(R record) {
    this(record,null);
  }

  public boolean hasTime() {
    return sourceTime != null;
  }

}
