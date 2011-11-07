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

import org.apache.commons.math.special.Gamma;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.cloudera.science.mgps.NFunction;
import com.cloudera.science.mgps.QFunction;

/**
 * A Pig UDF for calculating the Empirical Bayes Geometric Mean for the
 * multi-item association sets algorithm described in:
 * "Empirical bayes screening for multi-item associations", DuMouchel and
 * Pregibon (2001). Available from:
 * 
 * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.22.6251&rep=rep1&type=pdf
 */
public class EBGM extends EvalFunc<Double> {
  
  private static class DeltaFunction {
    private final double alpha;
    private final double beta;
  
    public DeltaFunction(double alpha, double beta) {
      this.alpha = alpha;
      this.beta = beta;
    }
    
    public double eval(int n, double e) {
      return Gamma.digamma(alpha + n) - Math.log(beta + e);
    }
  }

  private final DeltaFunction delta1;
  private final DeltaFunction delta2;
  private final QFunction q;
  
  public EBGM(String alpha1, String beta1, String alpha2, String beta2, String p) {
    this(Double.valueOf(alpha1), Double.valueOf(beta1), Double.valueOf(alpha2),
        Double.valueOf(beta2), Double.valueOf(p));
  }
  
  public EBGM(double alpha1, double beta1, double alpha2, double beta2, double p) {
    this.delta1 = new DeltaFunction(alpha1, beta1);
    this.delta2 = new DeltaFunction(alpha2, beta2);
    this.q = new QFunction(new NFunction(alpha1, beta1),
        new NFunction(alpha2, beta2), p);
  }
  
  public double eval(int n, double e) {
    double qval = q.eval(n, e);
    return Math.exp(qval * delta1.eval(n, e) + (1.0 - qval) * delta2.eval(n, e));
  }

  @Override
  public Double exec(Tuple input) throws IOException {
    int n = ((Number) input.get(0)).intValue();
    double e = ((Number) input.get(1)).doubleValue();
    return eval(n, e);
  }
}
