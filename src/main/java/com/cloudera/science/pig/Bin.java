/**
 * Copyright 2011 Cloudera Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.science.pig;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A Pig UDF that assigns values to bins based on its two arguments. The
 * first argument is a {@link DataBag} of numeric values that are assigned
 * bins based on the range of values specified in the second argument,
 * another {@code DataBag}.
 * 
 * <p>This function is usually used in conjunction with the {@link Quantile}
 * function in order to assign observations to the appropriate quantile,
 * using the quantile values calculated to determine the ranges.
 *
 */
public class Bin extends EvalFunc<DataBag> {

  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  private final BagFactory bagFactory = BagFactory.getInstance();

  @Override
  public DataBag exec(Tuple input) throws IOException {
    DataBag output = bagFactory.newDefaultBag();
    Object o1 = input.get(0);
    if (!(o1 instanceof DataBag)) {
      throw new IOException("Expected input to be a bag, but got: " + o1.getClass());
    }
    DataBag inputBag = (DataBag) o1;
    Object o2 = input.get(1);
    if (!(o2 instanceof DataBag)) {
      throw new IOException("Expected second input to be a bag, but got: " + o2.getClass());
    }
    List<Double> quantiles = getQuantiles((DataBag) o2);
    for (Tuple t : inputBag) {
      if (t != null && t.get(0) != null) {
        double val = ((Number)t.get(0)).doubleValue();
        int index = Collections.binarySearch(quantiles, val);
        if (index > -1) {
          t = tupleFactory.newTuple(ImmutableList.of(index, t.get(0)));
        } else {
          t = tupleFactory.newTuple(ImmutableList.of(-index - 1, t.get(0)));
        }
        output.add(t);
      }
    }
    return output;
  }

  private boolean isNumeric(byte pigType) {
    return pigType == DataType.DOUBLE || pigType == DataType.FLOAT ||
        pigType == DataType.INTEGER || pigType == DataType.LONG;
  }
  
  private byte checkField(FieldSchema field) throws FrontendException {
    if (field.type != DataType.BAG) {
      throw new IllegalArgumentException("Expected a bag; found: " +
          DataType.findTypeName(field.type));
    }
    if (field.schema.size() != 1) {
      throw new IllegalArgumentException("The bag must contain a single field");
    }
    byte bagType = field.schema.getField(0).type;
    if (bagType == DataType.TUPLE) {
      bagType = field.schema.getField(0).schema.getField(0).type;
    }
    if (!isNumeric(bagType)) {
      throw new IllegalArgumentException("The bag's field must be a numeric type");
    }
    return bagType;
  }
  
  @Override
  public Schema outputSchema(Schema input) {
    if (input.size() != 2) {
      throw new IllegalArgumentException("Expected two bags; input has != 2 fields");
    }
    try {
      byte binType = checkField(input.getField(0));
      byte quantileType = checkField(input.getField(1));
      if (quantileType != DataType.DOUBLE) {
        throw new IllegalArgumentException("Expected doubles for quantile bag");
      }
      
      List<FieldSchema> fields = Lists.newArrayList(new FieldSchema("bin", DataType.INTEGER),
          new FieldSchema("value", binType));
      Schema tupleSchema = new Schema(fields);
      
      FieldSchema tupleFieldSchema = new FieldSchema("t", tupleSchema,
          DataType.TUPLE);
      
      Schema bagSchema = new Schema(tupleFieldSchema);
      bagSchema.setTwoLevelAccessRequired(true);
      FieldSchema bagFieldSchema = new FieldSchema("b", bagSchema, DataType.BAG);
      return new Schema(bagFieldSchema);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<Double> getQuantiles(DataBag bag) throws ExecException {
    List<Double> quantiles = Lists.newArrayList();
    for (Tuple t : bag) {
      if (t != null && t.get(0) != null) {
        quantiles.add(((Number)t.get(0)).doubleValue());
      }
    }
    Collections.sort(quantiles);
    return quantiles;
  }
}
