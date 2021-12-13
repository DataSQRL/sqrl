/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.dataeng.sqml.physical.flink.aggregates;


import ai.dataeng.sqml.physical.flink.Row;

public class CountHandleFunction implements AggsHandleFunction {

  private final int inputIndex;
  private long count;

  public CountHandleFunction(int inputIndex) {
    this.inputIndex = inputIndex;
  }

  @Override
  public void open(/*StateDataViewStore store*/) throws Exception {}

  @Override
  public void accumulate(Row input) throws Exception {
    count++;
  }

  @Override
  public void retract(Row input) throws Exception {
    count--;
  }

  @Override
  public void merge(Row accumulator) throws Exception {
    count += (Long)accumulator.getValue(0);
  }

  @Override
  public void setAccumulators(Row accumulator) throws Exception {
    count = (Long)accumulator.getValue(0);
  }

  @Override
  public void resetAccumulators() throws Exception {
    count = 0L;
  }

  @Override
  public Row getAccumulators() throws Exception {
    return new Row(count);
  }

  @Override
  public Row createAccumulators() throws Exception {
    return new Row(0L);
  }

  @Override
  public Row getValue() throws Exception {
    return getAccumulators();
  }

  @Override
  public void cleanup() throws Exception {}

  @Override
  public void close() throws Exception {}
}
