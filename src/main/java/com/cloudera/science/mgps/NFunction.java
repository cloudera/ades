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

package com.cloudera.science.mgps;

import org.apache.commons.math.special.Gamma;
import org.apache.commons.math.util.MathUtils;

/**
 * An implementation of the Fn function described in DuMouchel and Pregibon's 2001
 * paper, "Empirical bayes screening for multi-item associations". Used by the
 * {@link EBGM} and {@link EBCI} Pig functions for scoring the item tuples.
 */
public class NFunction {
  private final double alpha;
  private final double beta;
  
  public NFunction(double alpha, double beta) {
    this.alpha = alpha;
    this.beta = beta;
  }
  
  public double eval(int n, double e) {
    double x = -n * Math.log(1 + beta / e);
    double y = -alpha * Math.log(1 + e / beta);
    double z = Gamma.logGamma(alpha + n);
    double d = Gamma.logGamma(alpha) + MathUtils.factorialLog(n);
    return Math.exp(x + y + z - d);
  }    
}