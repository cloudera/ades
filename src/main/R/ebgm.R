#
# Copyright 2011 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# An R script that uses the EM algorithm to find a maximal set of values
# of (alpha1, beta1, alpha2, beta2, p) for a given squashed input set
# of tuples (N, e, weight). Can be used either interactively or run from
# the command line:
#
#      Rscript ebgm.R <path_to_csv_file_from_step3>
#
# Requires the 'BB' R package to be installed on the system where it is run.
#
# By default, the script will run 20 iterations from different random starting
# points and keep the parameter settings that had the maximum log likelihood.

library(BB, quietly=TRUE)

# The main pdf that we are trying to optimize.
ebgm.f <- function(n, e, alpha, beta) {
  x <- (1 + beta / e)**(-n)
  y <- (1 + e / beta)**(-alpha)
  z <- lgamma(alpha + n) - lgamma(alpha) - lfactorial(n)
  x * y * exp(z)
}

# Derivate of ebgm.f with respect to alpha.
ebgm.f.da <- function(n, e, alpha, beta) {
  d <- digamma(alpha) - digamma(alpha + n) + log(1 + e / beta)
  -ebgm.f(n, e, alpha, beta) * d
}

# Derivative of ebgm.f with respect to beta.
ebgm.f.db <- function(n, e, alpha, beta) {
  x <- ebgm.f(n, e, alpha, beta)
  w <- exp(lgamma(alpha + n) - lgamma(alpha) - lfactorial(n))
  -x * w * (n / (e + beta) + alpha*e / beta**2)
}

# The normalizing denominator term, since we are using a truncated
# set of values of n (i.e., we do not evaluate ebgm.f at points where
# n == 0).
# 
# nstar is the minimal value of n in the data.
ebgm.g <- function(n, e, alpha, beta, nstar) {
  d <- sapply(0:(nstar - 1), function(x) {ebgm.f(x, e, alpha, beta)})
  1 - apply(d, 1, sum)
}

# A utility function for computing the derivative of ebgm.g.
ebgm.g.d <- function(n, e, alpha, beta, nstar, dx) {
  d.dx <- sapply(0:(nstar - 1), function(x) {-dx(x, e, alpha, beta)})
  apply(d.dx, 1, sum)
}

# Derivative of ebgm.g with respect to alpha.
ebgm.g.da <- function(n, e, alpha, beta, nstar) {
  ebgm.g.d(n, e, alpha, beta, nstar, ebgm.f.da)
}

# Derivative of ebgm.g with respect to beta.
ebgm.g.db <- function(n, e, alpha, beta, nstar) {
  ebgm.g.d(n, e, alpha, beta, nstar, ebgm.f.db)
}

# The overall distribution function for n, computed as the
# ratio of ebgm.f and ebgm.g at the given points.
ebgm.fstar <- function(n, e, alpha, beta, nstar) {
  ebgm.f(n, e, alpha, beta) / ebgm.g(n, e, alpha, beta, nstar)
}

# The log likelihood function for the mixture distribution.
ebgm.ll <- function(theta, n, e, w, nstar) {
  f1 <- ebgm.fstar(n, e, theta[1], theta[2], nstar)
  f2 <- ebgm.fstar(n, e, theta[3], theta[4], nstar)
  sum(w * log(theta[5] * f1 + (1 - theta[5]) * f2))
}

# The main optimization routine, which makes use of the EM algorithm.
ebgm.optim <- function(x, e, w, theta=c(rexp(4), runif(1)), toler=0.01) {
  d.loglik.alpha <- function(alpha, beta, N, E, W) {
    d1 <- ebgm.f.da(N, E, alpha, beta) * ebgm.g(N, E, alpha, beta, min(N))
    d2 <- ebgm.f(N, E, alpha, beta) * ebgm.g.da(N, E, alpha, beta, min(N))
    sum(W*(d1 - d2))
  }
  d.loglik.beta <- function(beta, alpha, N, E, W) {
    d1 <- ebgm.f.db(N, E, alpha, beta) * ebgm.g(N, E, alpha, beta, min(N))
    d2 <- ebgm.f(N, E, alpha, beta) * ebgm.g.db(N, E, alpha, beta, min(N))
    sum(W*(d1 - d2))
  }
  n <- length(x)
  nstar <- min(x)
  error <- 1
  alpha1 <- theta[1]
  beta1 <- theta[2]
  alpha2 <- theta[3]
  beta2 <- theta[4]
  pi.j <- c(theta[5], 1 - theta[5])
  theta.eb <- theta
  dnbs <- rbind(t(ebgm.f(x, e, alpha1, beta1)), t(ebgm.f(x, e, alpha2, beta2)))
  pi.ij0 <- diag(pi.j) %*% dnbs
  pi.ij <- matrix(NA, nrow = 2, ncol = n)
  for (i in 1:n) pi.ij[, i] <- pi.ij0[, i]/colSums(pi.ij0)[i]
  j <- 0
  cntrl <- list(noimp=10)
  while (error > toler && j < 100) {
    oldp <- pi.ij
    olda1 <- alpha1
    olda2 <- alpha2
    oldb1 <- beta1
    oldb2 <- beta2
    oldll <- ebgm.ll(c(olda1, oldb1, olda2, oldb2, pi.ij[1]), x, e, w, nstar)
    alpha1 <- BBsolve(olda1, d.loglik.alpha, beta=oldb1, N=x, E=e,
                      W=w*oldp[1,], control=cntrl, quiet=TRUE)$par
    beta1 <- BBsolve(oldb1, d.loglik.beta, alpha=alpha1, N=x, E=e,
                      W=w*oldp[1,], control=cntrl, quiet=TRUE)$par
    alpha2 <- BBsolve(olda2, d.loglik.alpha, beta=oldb2, N=x, E=e,
                      W=w*oldp[2,], control=cntrl, quiet=TRUE)$par
    beta2 <- BBsolve(oldb2, d.loglik.beta, alpha=alpha2, N=x, E=e,
                     W=w*oldp[2,], control=cntrl, quiet=TRUE)$par
    pi.j <- apply(oldp, 1, sum)/n
    theta.eb <- c(alpha1, beta1, alpha2, beta2, pi.j[1])
    if (any(theta.eb <= 0) | theta.eb[5] > 1) {
      stop("Invalid initial values, try a different set.")
    }
    loglik <- ebgm.ll(theta.eb, x, e, w, nstar)
    dnbs <- rbind(t(ebgm.f(x, e, alpha1, beta1)), t(ebgm.f(x, e, alpha2, beta2)))
    pi.ij0 <- diag(pi.j) %*% dnbs
    pi.ij <- matrix(NA, nrow=2, ncol=n)
    for (i in 1:n) pi.ij[, i] <- pi.ij0[, i]/colSums(pi.ij0)[i]
    error <- abs(oldll - loglik)
    j <- j + 1
  }
  list(theta=theta.eb,loglik=ebgm.ll(theta.eb, x, e, w, nstar))
}

# A main routine for running multiple iterations of the ebgm.optim
# function at multiple starting points.
ebgm.main <- function(file, iter=20) {
  data <- read.csv(file, header=F, sep=',')
  data <- data[data[[2]] >= 1, ]
  n <- data[[1]]
  w <- data[[4]]
  e <- data[[3]] / w
  max <- list(loglik=-Inf)
  for (i in 1:iter) {
    res <- tryCatch(ebgm.optim(n, e, w), error=function(ex) {NULL})
    if (!is.null(res) && res$loglik > max$loglik) {
      max <- res
    }
  }
  max
}

# Code for running this script from the command line, instead
# of interactively.
if (!interactive()) {
  args <- commandArgs()
  args <- args[(which(args == "--args") + 1):length(args)]
  if (length(args) != 1) {
  }
  opt <- ebgm.main(args[1])
  print(opt)
}
