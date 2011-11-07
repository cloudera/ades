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

/**
 * An implementation of the Qn function described in DuMouchel and Pregibon's 2001
 * paper, "Empirical bayes screening for multi-item associations". Used by the
 * {@link EBGM} and {@link EBCI} Pig functions for scoring the item tuples.
 *
 */
public class QFunction {
  private final NFunction n1;
  private final NFunction n2;
  private final double p;
  
  public QFunction(NFunction n1, NFunction n2, double p) {
    this.n1 = n1;
    this.n2 = n2;
    this.p = p;
  }
  
  public double eval(int n, double e) {
    double r1 = p * n1.eval(n, e);
    double r2 = (1.0 - p) * n2.eval(n, e);
    return r1 / (r1 + r2);
  }
}