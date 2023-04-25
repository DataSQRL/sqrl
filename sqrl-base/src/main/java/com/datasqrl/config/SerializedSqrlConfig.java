package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import java.io.Serializable;
import lombok.NonNull;

public interface SerializedSqrlConfig extends Serializable {

  public SqrlConfig deserialize(@NonNull ErrorCollector errors);

}
