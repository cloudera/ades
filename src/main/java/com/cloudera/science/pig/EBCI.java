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

import org.apache.commons.math.ConvergenceException;
import org.apache.commons.math.FunctionEvaluationException;
import org.apache.commons.math.MathRuntimeException;
import org.apache.commons.math.analysis.DifferentiableUnivariateRealFunction;
import org.apache.commons.math.analysis.UnivariateRealFunction;
import org.apache.commons.math.analysis.integration.SimpsonIntegrator;
import org.apache.commons.math.analysis.integration.UnivariateRealIntegrator;
import org.apache.commons.math.analysis.solvers.BrentSolver;
import org.apache.commons.math.distribution.GammaDistribution;
import org.apache.commons.math.distribution.GammaDistributionImpl;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.cloudera.science.mgps.NFunction;
import com.cloudera.science.mgps.QFunction;

public class EBCI extends EvalFunc<Double> {

  private static class PiFunction implements UnivariateRealFunction {
    private final double p;
    private final GammaDistribution g1;
    private final GammaDistribution g2;
    
    public PiFunction(double p, GammaDistribution g1, GammaDistribution g2) {
      this.p = p;
      this.g1 = g1;
      this.g2 = g2;
    }
    
    
    public double value(double lambda) throws FunctionEvaluationException {
      return p * g1.density(lambda) + (1.0 - p) * g2.density(lambda);
    }
  }
  
  private static class PiFunctionIntegral implements DifferentiableUnivariateRealFunction {
    private final PiFunction pi;
    private final double target;
    private final UnivariateRealIntegrator integrator;
    
    public PiFunctionIntegral(PiFunction pi, double target) {
      this.pi = pi;
      this.target = target;
      this.integrator = new SimpsonIntegrator();
    }
    
    public double value(double lambda) throws FunctionEvaluationException {
      try {
        if (lambda == 0.0) {
          return -target;
        }
        return integrator.integrate(pi, 0.0, lambda) - target;
      } catch (ConvergenceException e) {
        e.printStackTrace();
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      } catch (Exception e)
      {
        
    	  throw new RuntimeException("lambda-" + lambda,e);
      }
      return Double.POSITIVE_INFINITY;
    }

    public UnivariateRealFunction derivative() {
      return pi;
    }
    
  }
  
  private final double target;
  private final double alpha1;
  private final double beta1;
  private final double alpha2;
  private final double beta2;
  private final QFunction q;
  
  public EBCI(String target, String alpha1, String beta1,
      String alpha2, String beta2, String p) {
    this(Double.valueOf(target), Double.valueOf(alpha1), Double.valueOf(beta1),
        Double.valueOf(alpha2), Double.valueOf(beta2), Double.valueOf(p));
  }
  
  
  
  
  public EBCI(double target, double alpha1, double beta1,
      double alpha2, double beta2, double p) {
    this.target = target;
    this.alpha1 = alpha1;
    this.beta1 = beta1;
    this.alpha2 = alpha2;
    this.beta2 = beta2;
    this.q = new QFunction(new NFunction(alpha1, beta1),
        new NFunction(alpha2, beta2), p);
  }
  
  public double eval(int n, double e) {
    GammaDistribution g1 = new GammaDistributionImpl(alpha1 + n, beta1 + e);
    GammaDistribution g2 = new GammaDistributionImpl(alpha2 + n, beta2 + e);
    PiFunction pi = new PiFunction(q.eval(n, e), g1, g2);
    PiFunctionIntegral ipi = new PiFunctionIntegral(pi, target);
    
    
    
    try {
      return (new BrentSolver()).solve(ipi, 0.0, 10.0, 0.01);
    } catch (ConvergenceException e1) {
      e1.printStackTrace();
    } catch (FunctionEvaluationException e1) {
      e1.printStackTrace();
    } catch (RuntimeException e1)
    {
    	//MathRuntimeException function values at endpoints do not have different signs 
    	e1.printStackTrace();
		
    }
    return -1.0;
  }
  
  @Override
  public Double exec(Tuple input) throws IOException {
    int n = ((Number) input.get(0)).intValue();
    double e = ((Number) input.get(1)).doubleValue();
    return eval(n, e);
  }

}
