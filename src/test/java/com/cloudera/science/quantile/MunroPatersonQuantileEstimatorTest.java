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

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

public class MunroPatersonQuantileEstimatorTest extends TestCase {

  private QuantileEstimator create(int numQuantiles) {
    return new MunroPatersonQuantileEstimator(numQuantiles);
  }
  
  public void testBasic() {
    QuantileEstimator qe = create(6);
    for (int i = 0; i < 200; i += 2) {
      qe.add(i);
    }
    assertEquals(ImmutableList.of(0.0, 38.0, 78.0, 118.0, 158.0, 198.0),
        qe.getQuantiles());
  }
}
