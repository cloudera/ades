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
 * For the first step in our pipeline, we will take the input AERS data,
 * do some simple filtering, break the observations into strata based on
 * the patient's gender, age, and the date of the report, and then
 * count the occurrences of drugs, reactions, and drug-reaction pairs within
 * each of the strata.
 */

/**
 * Start by declaring the jar file that contains the user-defined functions
 * (UDFs) we will be using in this script. We also create an instance of the
 * Combinatorial UDF with an arity of 2 that we name 'Choose2'. Given an input
 * bag of data, Choose2 will generate the unique 2-element subsets of that bag.
 */
REGISTER 'aers.jar';
DEFINE Choose2 com.cloudera.science.pig.Combinatorial('2');

/**
 * Next, load the data files from the HDFS directory where they are stored.
 * Note that we use '$' in the PigStorage function because the fields are
 * separated by the '$' character. We also provide a schema statement that
 * names each of the fields in the files, even though we will only need to
 * use a subset of these fields.
 */
drugs = LOAD 'aers/drugs' USING PigStorage('$') AS (
    isr: long, drug_seq: long, role: chararray, name: chararray,
    vbm: long, route: chararray, dose_vbm: chararray, dechal: chararray, rechal: chararray,
    lot: long, exp_dt: chararray, nda: long);
demos = LOAD 'aers/demos' USING PigStorage('$') AS (
    isr: long, case: int, if_cod: chararray, foll_seq: chararray, image: chararray,
    event_dt: chararray, mfr_dt: chararray, fda_dt: chararray, rept_code: chararray,
    mfr_num: chararray, mfr_sndr: chararray, age: long, age_code: chararray,
    gender: chararray, e_sub: chararray, weight: long, wt_code: chararray,
    rept_dt: chararray, occp_code: chararray, death_dt: chararray, to_mfr: chararray,
    confid: chararray, reporter_country: chararray);
reacs = LOAD 'aers/reactions' USING PigStorage('$') AS (isr: long, code: chararray);

/**
 * Now we need to filter the demographic records to remove any records that we
 * want to exclude. In this statement, we are removing all observations where the
 * gender of the patient is unknown, where their age is either invalid or greater than
 * 100, or where the date associated with the report is before 2008.
 */
demos = FILTER demos BY age_code == 'YR' and (gender == 'M' or gender == 'F') and
    SUBSTRING(fda_dt, 0, 4) >= '2008' and (age > 0 and age <= 100);

/**
 * Each of the demographics records contains a case identifier. ISRs with the same
 * case identifier refer to the same patient, so we want to choose a single ISR for
 * each case number that will be the representative report for that patient. This
 * avoids double-counting the same underlying drug-reaction combinations multiple times
 * for the same person.
 * 
 * For simplicity, we simply choose the minimum ISR number for each case identifier
 * as the representative report for that patient.
 */
demos_by_case = GROUP demos BY case;
selected = FOREACH demos_by_case GENERATE MIN(demos.isr) as isr;

/**
 * Now we filter the demographics data to only include the reports that we selected
 * in the previous step. We also filter the fields in the demographic data to only
 * include the ones that we want to use for definin the strata- the patient's gender,
 * the patient's age, and the date associated with the report.
 */
selected_demos_join = JOIN demos BY isr, selected BY isr;
selected_demos = FOREACH selected_demos_join GENERATE demos::isr as isr, gender,
    age as age_bucket, fda_dt as time_bucket;

/**
 * This step begins the process of generating the counts of drugs, reactions,
 * drug-reaction pairs, and drug-drug-reaction triples. Each of these counts is
 * also grouped by strata.
 *
 * The COGROUP operator is like the GROUP operator, except it allows you to perform
 * the grouping operation on multiple datasets, instead of just one. In this case,
 * we are grouping the drugs, reacs, and selected_demos datasets together, such that
 * all of the records from each of those datasets that have the same ISR will be
 * combined together into a single, complex record.
 */
drugs_reacs_demos_by_isr = COGROUP drugs BY isr, reacs BY isr, selected_demos BY isr;

/**
 * We filter the records to ensure that we only look at the records that have
 * data from all of the cogrouped datasets.
 */
filtered_drugs_reacs_demos = FILTER drugs_reacs_demos_by_isr BY not IsEmpty(drugs) and
    not IsEmpty(reacs) and not IsEmpty(selected_demos);

/**
 * Use the cogrouped dataset to generate all of the combinations of drugs, reactions,
 * and demographic buckets, using Pig's flatten function, which creates an output record
 * for each of the values inside of a bag in a complex record. Since we are flattening
 * multiple bags in these statements, this operation generates the cross product of the
 * set of drug names, the set of reactions, and the demographic fields associated with
 * each cogrouped record.
 */
drugs1_reacs = FOREACH filtered_drugs_reacs_demos GENERATE flatten(drugs.name) as drug,
    flatten(reacs.code) as reac, flatten(selected_demos);
/**
 * Does the same thing as the prior statement, but uses the Choose2 function to generate
 * all of the two-drug subsets from the collection of drugs associated with this cogrouped
 * record before taking the cross product with the reactions and demographic fields.
 */
drugs2_reacs = FOREACH filtered_drugs_reacs_demos GENERATE flatten(Choose2(drugs.name)) as (d1, d2),
    flatten(reacs.code) as reac, flatten(selected_demos);

/**
 * The following statements compute aggregate counts of reported records, grouped by various
 * keys. In the next stage of the pipeline, we will join the outputs of these statements
 * together in order to calculate the actual and expected counts for each of the drug-drug-reaction
 * triples in the dataset.
 *
 * Each of the statements below calculates the distinct number of ISRs for each of the grouping
 * keys, using Pig's special syntax for COUNT DISTINCT operations.
 */

/* Compute the overall counts for each strata. */
demo_counts_group = GROUP drugs1_reacs BY (gender, age_bucket, time_bucket);
strat_demo_counts = FOREACH demo_counts_group {
 isrs = drugs1_reacs.isr;
 uniq = DISTINCT isrs;
 GENERATE group.gender as gender, group.age_bucket as age_bucket,
     group.time_bucket as time_bucket, COUNT(uniq);
}

/* Compute the counts for each drug within each strata. */
strat_drugs_group = GROUP drugs1_reacs BY (gender, age_bucket, time_bucket, drug);
strat_drugs_counts = FOREACH strat_drugs_group {
  isrs = drugs1_reacs.isr;
  uniq = DISTINCT isrs;
  GENERATE group.gender, group.age_bucket, group.time_bucket, group.drug, COUNT(uniq);
}
  
/* Compute the counts for each reaction within each strata. */
strat_reacs_group = GROUP drugs1_reacs BY (gender, age_bucket, time_bucket, reac);
strat_reacs_counts = FOREACH strat_reacs_group {
  isrs = drugs1_reacs.isr;
  uniq = DISTINCT isrs;
  GENERATE group.gender, group.age_bucket, group.time_bucket, group.reac, COUNT(uniq);
}

/* Compute the counts for each drug-reaction pair within each strata. */
strat_drugs_reacs_group = GROUP drugs1_reacs BY (gender, age_bucket, time_bucket,
    drug, reac);
strat_drugs_reacs_counts = FOREACH strat_drugs_reacs_group {
  isrs = drugs1_reacs.isr;
  uniq = DISTINCT isrs;
  GENERATE group.gender, group.age_bucket, group.time_bucket, group.drug,
      group.reac, COUNT(uniq);
}

/* Compute the counts for each drug-drug-reaction triple within each strata. */
drugs2_reacs_group = GROUP drugs2_reacs BY
    (gender, age_bucket, time_bucket, d1, d2, reac);
strat_drugs2_reacs_counts = FOREACH drugs2_reacs_group {
  isrs = drugs2_reacs.isr;
  uniq = DISTINCT isrs;
  GENERATE group.gender, group.age_bucket, group.time_bucket, group.d1, group.d2,
      group.reac, COUNT(uniq);
}

/**
 * Finally, we store the output of the previous jobs into directories under
 * our main aers/ directory.
 */
STORE drugs1_reacs INTO 'aers/strat_drugs1_reacs' using PigStorage('$');
STORE drugs2_reacs INTO 'aers/strat_drugs2_reacs' using PigStorage('$');
STORE strat_demo_counts INTO 'aers/strat_demo_counts' using PigStorage('$');
STORE strat_drugs_counts INTO 'aers/strat_drugs_counts' using PigStorage('$');
STORE strat_reacs_counts INTO 'aers/strat_reacs_counts' using PigStorage('$');
STORE strat_drugs_reacs_counts INTO 'aers/strat_drugs_reacs_counts' using PigStorage('$');
STORE strat_drugs2_reacs_counts INTO 'aers/strat_drugs2_reacs_counts' using PigStorage('$');
