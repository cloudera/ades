# Adverse Drug Event Analysis with Hadoop, R, and Gephi

## Introduction

## Prerequistes

### Code

This analysis is designed to be small enough that you can run it on a single machine if you
do not have access to a Hadoop cluster. You will need to have a version of [CDH3](https://ccp.cloudera.com/display/CDHDOC/CDH3+Installation)
on your local machine, along with the version of Pig that is compatible with that version.

You will need to have Maven for compiling the Pig user-defined functions, and may also want to have a
copy of [R](http://www.r-project.org/) and [Gephi](http://gephi.org/) for certain phases of the analysis.

### Data
The input data for this analysis may be downloaded from the FDA's [AERS website](http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083765.htm). You'll need to get the ASCII version of the data files for as many quarters as you would like to run over. For my own analysis, I used the data from 2008 through 2010.

The Pig scripts below assume that the input data is stored in three HDFS directories under
the user's home directory: aers/drugs, aers/demos, and aers/reactions. All of the DRUG*.TXT
files from the AERS website should go into aers/drugs, all of the DEMO*.TXT files should go
into aers/demos, and all of the REAC*.TXT files should go into aers/reactions.

## Running the Pipeline

If you have not done so already, load the input data into the Hadoop cluster:

	hadoop fs -mkdir aers
	hadoop fs -mkdir aers/drugs
	hadoop fs -put DRUG*.TXT aers/drugs
	hadoop fs -mkdir aers/demos
	hadoop fs -put DEMO*.TXT aers/demos
	hadoop fs -mkdir aers/reactions
	hadoop fs -put REAC*.TXT aers/reactions

Each of these commands should be run from the project's top-level directory,
i.e., the directory that contains this README file.

	mvn package  # Builds the Pig UDFs
	pig -f src/main/pig/step1_join_drugs_reactions.pig
	pig -f src/main/pig/step2_generate_drug_reaction_counts.pig
	pig -f src/main/pig/step3_generate_squashed_distribution.pig

At this point, you can optionally run the R code to solve the MGPS
optimization problem. You will need to install the *BB* library in your
local version of R using *install.packages()* if you do not have it already.

	hadoop fs -getmerge aers/drugs2_reacs_stats d2r_stats.csv
	Rscript src/main/R/ebgm.R d2r_stats.csv

The output from the optimization run may be plugged into the Pig script
that scores the tuples, or you can just use the default parameters that
are there now:

	pig -f src/main/pig/step4_apply_ebgm.pig

The final output will be in *aers/scored_drugs2_reacs*. To generate the GEXF
file of drug-drug interactions to load into Gephi, run:

	hadoop fs -getmerge aers/scored_drugs2_reacs scored_d2r.csv
	./src/main/python/gephi.py scored_d2r.csv > drugs.gexf
