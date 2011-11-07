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
 * In the second stage of the pipeline, we join the datasets we created
 * in the first stage in order to compute the actual and expected counts
 * for each drug-drug-reaction triple that we observed in the dataset.
 *
 * The expected count for each triple is based on an independence
 * assumption: the probability of a particular drug-drug-reaction triple
 * appearing in an observation is simply the product of the frequencies
 * of the individual drugs and the reaction within the strata that
 * contains that observation.
 */

/**
 * Load the datasets that we generated in step 1.
 */
demo_counts = LOAD 'aers/strat_demo_counts' USING PigStorage('$') as (
  gender: chararray, age_bucket: double, time_bucket: chararray,
  count: long
);
drug_counts = LOAD 'aers/strat_drugs_counts' USING PigStorage('$') as (
  gender: chararray, age_bucket: double, time_bucket: chararray,
  drug: chararray, count: long
);
reac_counts = LOAD 'aers/strat_reacs_counts' USING PigStorage('$') as (
  gender: chararray, age_bucket: double, time_bucket: chararray,
  reac: chararray, count: long
);
d2r_count = LOAD 'aers/strat_drugs2_reacs_counts' USING PigStorage('$') as (
  gender: chararray, age_bucket: double, time_bucket: chararray,
  d1: chararray, d2: chararray, reac: chararray, dr_count: long
);

/* Join the drugs-drugs-reactions triples with the total counts for
 * each strata.
 */
total_join = JOIN d2r_count BY (gender, age_bucket, time_bucket),
    demo_counts by (gender, age_bucket, time_bucket);
total = FOREACH total_join GENERATE d2r_count::gender as gender,
    d2r_count::age_bucket as age_bucket, d2r_count::time_bucket as time_bucket,
    d1 as d1, d2 as d2, reac as reac, dr_count as dr_count,
    demo_counts::count as total_count;

/* Join to get the occurrences of drug d1. */
total_d1_join = JOIN total BY (gender, age_bucket, time_bucket, d1),
    drug_counts by (gender, age_bucket, time_bucket, drug);
total_d1 = FOREACH total_d1_join GENERATE total::gender as gender,
    total::age_bucket as age_bucket, total::time_bucket as time_bucket,
    d1 as d1, d2 as d2, reac as reac, dr_count as dr_count,
    total_count as total_count, drug_counts::count as d1_count;

/* Join to get the occurrences of drug d2. */
total_d1d2_join = JOIN total_d1 BY (gender, age_bucket, time_bucket,
    d2), drug_counts by (gender, age_bucket, time_bucket, drug);
total_d1d2 = FOREACH total_d1d2_join GENERATE total_d1::gender as gender,
    total_d1::age_bucket as age_bucket, total_d1::time_bucket as time_bucket,
    d1 as d1, d2 as d2, reac as reac, dr_count as dr_count,
    total_count as total_count, d1_count as d1_count,
    drug_counts::count as d2_count;

/* And finally, join to get the reaction counts. */
total_d1d2r_join = JOIN total_d1d2 BY (gender, age_bucket, time_bucket,
    reac), reac_counts by (gender, age_bucket, time_bucket, reac);
total_d1d2r = FOREACH total_d1d2r_join GENERATE total_d1d2::gender as gender,
    total_d1d2::age_bucket as age_bucket, total_d1d2::time_bucket as time_bucket,
    d1 as d1, d2 as d2, total_d1d2::reac as reac, dr_count as dr_count,
    total_count as total_count, d1_count as d1_count, d2_count as d2_count,
    reac_counts::count as reac_count;

/** 
 * Generate expected counts for each drug-drug-reaction triple within
 * within each strata. Note that the expected count for each triple is
 * (total) * (d1/total) * (d2/total) * (reac/total) = (d1*d2*reac)/(total*total).
 */
actual_expected = FOREACH total_d1d2r GENERATE gender, age_bucket,
    time_bucket, d1, d2, reac, dr_count as actual,
    (d1_count * d2_count * reac_count) / (1.0 * total_count * total_count) as expected;

/**
 * Finally, sum the actual and expected counts across strata, grouping them by
 * (d1, d2, reac) in order to get the overall counts for each triple.
 */
actual_expected_group = GROUP actual_expected BY (d1, d2, reac);
final = FOREACH actual_expected_group GENERATE group.d1 as d1, group.d2 as d2,
    group.reac as reac, SUM(actual_expected.actual) as actual,
    SUM(actual_expected.expected) as expected;

STORE final INTO 'aers/drugs2_reacs_actual_expected' USING PigStorage('$');
