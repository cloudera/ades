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
package com.cloudera.science.quantile;

import java.util.List;

/**
 * Abstract base class for quantile estimation algorithms that are used by the
 * {@link Quantile} Pig UDF.
 *
 */
public abstract class QuantileEstimator {

  protected final int numQuantiles;

  /**
   * Base constructor for quantile estimators that specifies the number of
   * quantiles to construct.
   * 
   * @param numQuantiles The number of quantiles to construct
   */
  public QuantileEstimator(int numQuantiles) {
    this.numQuantiles = Math.max(2, numQuantiles);
  }
  
  /**
   * Adds the given point to the set of points used to estimate the quantiles.
   * 
   * @param point The point to add
   */
  public abstract void add(double point);
  
  /**
   * Resets the state of this estimator.
   */
  public abstract void clear();
  
  /**
   * Returns the estimated quantile based on the data that has been added.
   */
  public abstract List<Double> getQuantiles();
}
