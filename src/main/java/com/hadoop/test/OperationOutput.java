/**
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

package com.hadoop.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;

/**
 * An operation output has the following object format whereby simple types are
 * represented as a key of dataType:operationType*measurementType and these
 * simple types can be combined (mainly in the reducer) using there given types
 * into a single operation output.
 * 
 * Combination is done based on the data types and the following convention is
 * followed (in the following order). If one is a string then the other will be
 * concated as a string with a ";" separator. If one is a double then the other
 * will be added as a double and the output will be a double. If one is a float
 * then the other will be added as a float and the the output will be a float.
 * Following this if one is a long the other will be added as a long and the
 * output type will be a long and if one is a integer the other will be added as
 * a integer and the output type will be an integer.
 */
class OperationOutput {

  private OutputType dataType;
  private String opType, measurementType;
  private Object value;
  private long count = 1;

  private static final String TYPE_SEP = ":";
  private static final String MEASUREMENT_SEP = "*";
  private static final String STRING_SEP = ";";

  enum OutputType {
    STRING, FLOAT, LONG, DOUBLE, INTEGER
  }

  /**
   * Parses a given key according to the expected key format and forms the given
   * segments.
   * 
   * @param key
   *          the key in expected dataType:operationType*measurementType format
   * @param value
   *          a generic value expected to match the output type
   * @throws IllegalArgumentException
   *           if invalid format
   */
  OperationOutput(String key, Object value) {
    int place = key.indexOf(TYPE_SEP);
    if (place == -1) {
      throw new IllegalArgumentException(
          "Invalid key format - no type separator - " + TYPE_SEP);
    }
    try {
      dataType = OutputType.valueOf(
          StringUtils.toUpperCase(key.substring(0, place)));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid key format - invalid output type", e);
    }
    key = key.substring(place + 1);
    place = key.indexOf(MEASUREMENT_SEP);
    if (place == -1) {
      throw new IllegalArgumentException(
          "Invalid key format - no measurement separator - " + MEASUREMENT_SEP);
    }
    opType = key.substring(0, place);
    measurementType = key.substring(place + 1);
    this.value = value;
  }

  OperationOutput(Text key, Object value) {
    this(key.toString(), value);
  }

  public String toString() {
    return getKeyString() + " (" + this.value + ")";
  }

  OperationOutput(OutputType dataType, String opType, String measurementType,
                  Object value) {
    this(dataType, opType, measurementType, value, 1);
  }

  OperationOutput(OutputType dataType, String opType, String measurementType,
                  Object value, long count) {
    this.dataType = dataType;
    this.opType = opType;
    this.measurementType = measurementType;
    this.value = value;
    this.count = count;
  }

  /**
   * Formats the key for output
   * 
   * @return String
   */
  private String getKeyString() {
    StringBuilder str = new StringBuilder();
    str.append(getOutputType().name());
    str.append(TYPE_SEP);
    str.append(getOperationType());
    str.append(MEASUREMENT_SEP);
    str.append(getMeasurementType());
    return str.toString();
  }

  /**
   * Retrieves the key in a hadoop text object
   * 
   * @return Text text output
   */
  Text getKey() {
    return new Text(getKeyString());
  }

  /**
   * Gets the output value in text format
   * 
   * @return Text
   */
  Text getOutputValue() {
    StringBuilder valueStr = new StringBuilder();
    valueStr.append(getValue());
    return new Text(valueStr.toString());
  }

  /**
   * Gets the object that represents this value (expected to match the output
   * data type)
   * 
   * @return Object
   */
  Object getValue() {
    return value;
  }

  /**
   * Gets the output data type of this class.
   */
  OutputType getOutputType() {
    return dataType;
  }

  /**
   * Gets the operation type this object represents.
   * 
   * @return String
   */
  String getOperationType() {
    return opType;
  }

  /**
   * Gets the measurement type this object represents.
   * 
   * @return String
   */
  String getMeasurementType() {
    return measurementType;
  }

  /**
   * Gets the count of operations.
   * 
   * @return long
   */
  long getCount() {
    return count;
  }

}
