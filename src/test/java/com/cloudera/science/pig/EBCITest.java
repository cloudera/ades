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

import junit.framework.TestCase;

public class EBCITest extends TestCase {

  private static final double TOL = 0.001;
  
  public void testBasic() throws Exception {
    EBCI ebci = new EBCI(0.05, 0.000010000, 0.08233474, 3.32444654, 3.33311009,
        1.00000000);
    int n = 20;
    double e = 0.234;
    assertEquals(4.1929, ebci.eval(n, e), TOL);
  }
}
