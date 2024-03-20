package com.datasqrl.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@NoArgsConstructor
@Getter
public class LogicalStreamMetaData implements Serializable {

  int[] keyIdx;

  int[] selectIdx;

  int timestampIdx;

}
