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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 * A pig function that can be used to generate all subsets of a certain size
 * from a bag.
 * 
 * <p>It expects that the bag's tuple contains only a single field, and
 * that the type of that field implements {@link Comparable}. The returned
 * value is a bag of {@link Tuple} values with N-fields, where N is the arity
 * specified in the constructor.
 * 
 */
public class Combinatorial extends EvalFunc<DataBag> {

  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  private final BagFactory bagFactory = BagFactory.getInstance();
  private final int arity;
  
  public Combinatorial(String arity) {
    this.arity = Integer.valueOf(arity);
  }
  
  @Override
  public DataBag exec(Tuple input) throws IOException {
    try {
      DataBag output = bagFactory.newDefaultBag();
      Object o = input.get(0);
      if (!(o instanceof DataBag)) {
        throw new IOException("Expected input to be a bag, but got: " + o.getClass().getName());
      }
      DataBag inputBag = (DataBag) o;
      Set<Comparable> uniqs = Sets.newTreeSet();
      for (Tuple t : inputBag) {
        if (t != null && t.get(0) != null) {
          uniqs.add((Comparable) t.get(0));
        }
      }
      if (uniqs.size() < arity) {
        return output;
      }
      List<Comparable> values = Lists.newArrayList(uniqs);
      Comparable[] subset = new Comparable[arity];
      
      process(values, subset, 0, 0, output);

      return output;
    } catch (ExecException e) {
      throw new IOException(e);
    }
  }

  private void process(List<Comparable> values, Comparable[] subset, int curSubsetSize, int nextIndex,
      DataBag output) {
    if (curSubsetSize == subset.length) {
      output.add(tupleFactory.newTuple(Arrays.asList(subset)));
    } else {
      for (int i = nextIndex; i < values.size(); i++) {
        subset[curSubsetSize] = values.get(i);
        process(values, subset, curSubsetSize + 1, i + 1, output);
      }
    }
  }
  
  private boolean isComparable(byte pigType) {
    return DataType.isAtomic(pigType) || pigType == DataType.GENERIC_WRITABLECOMPARABLE;
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
      if (input.getField(0).schema.getField(0).type != DataType.TUPLE) {
          throw new IllegalArgumentException("Expected a tuple in a bag; found: " +
              DataType.findTypeName(input.getField(0).type));
        }
      if (input.getField(0).schema.size() != 1) {
        throw new IllegalArgumentException("The bag must contain a single field");
      }
      
      Schema bagSchema = input.getField(0).schema;
      Schema tupleSchema = bagSchema.getField(0).schema;      

      byte fieldType = tupleSchema.getField(0).type;
            
      if (!isComparable(fieldType)) {
        throw new IllegalArgumentException("The bag's Tulple's field must be a comparable type");
      }
      
      //What does this code want to do ??
      FieldSchema inputField = tupleSchema.getField(0);
      String inputName = inputField.alias;
      List<FieldSchema> fields = Lists.newArrayList();
      for (int i = 0; i < arity; i++) {
        fields.add(new FieldSchema(inputName + i, inputField.type));
      }
      Schema newTupleSchema = new Schema(fields);
      
      FieldSchema tupleFieldSchema = new FieldSchema(inputName + "tuple", newTupleSchema,
          DataType.TUPLE);
      
      Schema newBagSchema = new Schema(tupleFieldSchema);
      //bagSchema.setTwoLevelAccessRequired(true);
      Schema.FieldSchema bagFieldSchema = new Schema.FieldSchema(inputName + "bag",
    		  newBagSchema, DataType.BAG);
      return new Schema(bagFieldSchema);
      
      /*
      byte bagType = input.getField(0).schema.getField(0).type;
      if (bagType == DataType.TUPLE) {
        bagType = input.getField(0).schema.getField(0).schema.getField(0).type;
      }
      if (!isComparable(bagType)) {
        throw new IllegalArgumentException("The bag's field must be a comparable type");
      }
      
      FieldSchema inputField = input.getField(0).schema.getField(0);
      String inputName = inputField.alias;
      List<FieldSchema> fields = Lists.newArrayList();
      for (int i = 0; i < arity; i++) {
        fields.add(new FieldSchema(inputName + i, inputField.type));
      }
      Schema tupleSchema = new Schema(fields);
      
      FieldSchema tupleFieldSchema = new FieldSchema(inputName + "tuple", tupleSchema,
          DataType.TUPLE);
      
      Schema bagSchema = new Schema(tupleFieldSchema);
      bagSchema.setTwoLevelAccessRequired(true);
      Schema.FieldSchema bagFieldSchema = new Schema.FieldSchema(inputName + "bag",
          bagSchema, DataType.BAG);
      return new Schema(bagFieldSchema);
      */
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
