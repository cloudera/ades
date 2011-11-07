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
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.cloudera.science.quantile.MunroPatersonQuantileEstimator;
import com.cloudera.science.quantile.QuantileEstimator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A Pig UDF for computing quantiles from a {@link DataBag} of numeric values.
 * It returns a {@code DataBag} of {@link Tuple} values with two fields:
 * a zero-indexed integer that represents the quantile and a double value
 * that is actual quantile estimate.
 * 
 * <p>By default, this class uses the Munro-Paterson algorithm for estimating
 * quantiles in a streaming fashion. Subclasses may override the
 * {@code createEstimator} method in this class to return a different
 * implementation.
 *
 */
public class Quantile extends EvalFunc<DataBag> {

  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  private final BagFactory bagFactory = BagFactory.getInstance();
  private final int numQuantiles;
  
  public Quantile(String numQuantiles) {
    this.numQuantiles = Integer.valueOf(numQuantiles);
  }
  
  protected QuantileEstimator createEstimator() {
    return new MunroPatersonQuantileEstimator(numQuantiles);
  }
  
  @Override
  public DataBag exec(Tuple input) throws IOException {
    DataBag output = bagFactory.newDefaultBag();
    Object o = input.get(0);
    if (!(o instanceof DataBag)) {
      throw new IOException("Expected input to be a bag, but got: " + o.getClass().getName());
    }
    DataBag inputBag = (DataBag) o;
    QuantileEstimator estimator = createEstimator();
    
    for (Tuple t : inputBag) {
      if (t != null && t.get(0) != null) {
        estimator.add(((Number) t.get(0)).doubleValue());
      }
    }
    
    List<Double> quantiles = estimator.getQuantiles();
    for (int i = 0; i < quantiles.size(); i++) {
      output.add(tupleFactory.newTuple(ImmutableList.of(i, quantiles.get(i))));
    }
    return output;
  }

  private boolean isNumeric(byte pigType) {
    return pigType == DataType.DOUBLE || pigType == DataType.FLOAT ||
        pigType == DataType.INTEGER || pigType == DataType.LONG;
  }
  
  @Override
  public Schema outputSchema(Schema input) {
    if (input.size() != 1) {
      throw new IllegalArgumentException("Expected a bag; input has > 1 field");
    }
    try {
      if (input.getField(0).type != DataType.BAG) {
        throw new IllegalArgumentException("Expected a bag; found: " +
            DataType.findTypeName(input.getField(0).type));
      }
      if (input.getField(0).schema.size() != 1) {
        throw new IllegalArgumentException("The bag must contain a single field");
      }
      byte bagType = input.getField(0).schema.getField(0).type;
      if (bagType == DataType.TUPLE) {
        bagType = input.getField(0).schema.getField(0).schema.getField(0).type;
      }
      if (!isNumeric(bagType)) {
        throw new IllegalArgumentException("The bag's field must be a numeric type");
      }
      
      List<FieldSchema> fields = Lists.newArrayList(new FieldSchema("quantile", DataType.INTEGER),
          new FieldSchema("value", DataType.DOUBLE));
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
}
