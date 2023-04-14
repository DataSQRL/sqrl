package com.datasqrl.model;

import java.io.Serializable;
import lombok.Value;

@Value
public class LogicalStreamMetaData implements Serializable {

  final int[] keyIdx;

  final int[] selectIdx;

  final int timestampIdx;

}
