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

/**
 * The purpose of this stage is to generate a "squashed" version of the
 * data that will be used to fit the Empirical Bayes model that we use
 * to score each of the drug-drug-reaction triples based on how surprising
 * they are.
 *
 * The input to this stage is the set of (actual count, expected count)
 * pairs for each triple that was the output of step 2. For each distinct
 * value of actual count, we want to get a compressed version of the
 * distribution of expected counts that occur with that actual count.
 * The most straightforward way to do this is to compute the quantiles of
 * the expected counts for each of the actual counts, and then compute the
 * mean expected count for each (actual_count, quantile_bucket) combination.
 */

/**
 * Register the jar file that contains our UDFs, and provide definitions
 * for the Quantiles function (with 11 bins) and the Bin function, which
 * uses the output of the Quantiles function to assign the values in the
 * original data set into ten bins that are delimited by the quantile
 * values.
 */
REGISTER 'target/ades-0.1.0-jar-with-dependencies.jar';
DEFINE Quantiles com.cloudera.science.pig.Quantile('11');
DEFINE Bin com.cloudera.science.pig.Bin();

/**
 * A macro value that sets the minimum support a drug-drug-reaction
 * triple must have for us to consider it. Setting this higher dramatically
 * reduces the amount of data that we have to consider, and makes the job
 * run more quickly.
 *
 * This parameter can be overridden at the command line by using Pig's -p
 * option to specify an alternate value, e.g.,
 *
 * pig -p FILTER_BELOW=5 -f step3_generate_squashed_distribution.pig
 *
 * would filter out any triple that did not have at least 5 actual observations.
 */ 
%default FILTER_BELOW 3;

/**
 * Load the data from the previous stage and then apply the minimum support
 * filter.
 */
data = LOAD 'aers/drugs2_reacs_actual_expected' USING PigStorage('$') as (
  d1: chararray, d2: chararray, reac: chararray,
  actual: long, expected: double);
filtered = FILTER data BY actual >= $FILTER_BELOW;

/**
 * Group the triples by their actual counts, and then use the approximate
 * quantiles algorithm to get a distribution of the expected counts for each
 * actual count.
 */
actual_group = GROUP filtered BY actual;
quantiles = FOREACH actual_group GENERATE group as actual,
    flatten(Quantiles(filtered.expected));

/**
 * Here we make use of the COGROUP operator again in order to execute an
 * operation that is difficult to perform in SQL: assigning a bucket to
 * an observation based on a set of ranges that are stored in a different
 * table.
 *
 * The Bin UDF below iterates through the values in the first argument,
 * and assigns the value to a bin based on which interval of the second
 * argument that value falls into.
 */
table_q = COGROUP filtered BY actual, quantiles BY actual;
table_bins = FOREACH table_q GENERATE group as actual,
    flatten(Bin(filtered.expected, quantiles.value)) as (bin, value);

/**
 * Finally, group the values by the actual count and the bin they were assigned
 * to, and compute the sum of the values and the total number of observations in
 * each bin to use as input to the external optimization routine.
 */
table_blocks = GROUP table_bins BY (actual, bin);
stats = FOREACH table_blocks GENERATE group.actual as actual, group.bin as bin,
    SUM(table_bins.value) as expected, COUNT(table_bins) as weight;

/**
 * Write the stats into an output directory, using a ',' instead of the '$'
 * separator since there are no textual fields in this output.
 */
STORE stats INTO 'aers/drugs2_reacs_stats' USING PigStorage(',');
