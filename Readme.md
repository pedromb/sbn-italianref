### Social and Behavioral Networks

## Italian Referendum 2016

### Authors

- Pedro Magalhāes Bernardo
- Amir Abbasinejad

## How to Run

### 1. Clone the repository

Use the download link above or run the command:

```
git clone https://github.com/pedromb/sbn-italianref.git
```

### 2. Provide the datasets

Under /src/main/resources include the following files:

- Official_SBN-ITA-2016-Net.gz - File with connected graph.
- stream/ - Folder with the tweet data. The files are assumed to be compressed (gzip).

### 3. Run

You can execute using maven, just go to the root folder of the project and run:

```
mvn exec:java

```

P.S.: The first execution might take awhile since it has to generate the Lucene index. The indexing process can take up to 30min to be finished. Running the program for a second time should be faster. Also, the result of running the KPP-NEG algorithm is included here so it doesnt have to be ran again, since this process can take more than 1 day.

### 4. Graphs on report

The graphs on the report were generated using a Python script.
You can find the IPython notebook for that on generate_graphs.ipynb


### 5. Files Generated

The following files are generated after executing:

1. lucene_index/ - The folder with the Lucene index to manage the data.
2. temporal_analysis/ - The folder with data generated for the temporal analysis part.
    2.1 temporal_analysis/tweets_distribution.csv - Distribution of tweets by support (YES/NO) over time.
    2.2 temporal_analysis/clusters.csv - Components of each cluster for YES and NO supporters.
    2.3 temporal_analysis/graphs/no - Graph, largest connected components and k-core for each cluster of NO supporters
    2.4 temporal_analysis/graphs/yes - Graph, largest connected components and k-core for each cluster of YES supporters
    2.5 temporal_analysis/k_core_timeseries_no.csv - Distribution of tweets over time for the k-core components of each cluster of NO supporters.
    2.6 temporal_analysis/k_core_timeseries_yes.csv - Distribution of tweets over time for the k-core components of each cluster of YES supporters.
3. identifying_yes_no_supporters/ - The folder with data generated for the identitying yes and no supporters analysis part.
    3.1. identifying_yes_no_supporters/users_support.csv - Users and number of tweets produced by each one, by suport.
    3.2. identifying_yes_no_supporters/users_hits_yes.csv - Result of running HITS on YES supporters.
    3.3. identifying_yes_no_supporters/users_hits_no.csv - Result of running HITS on NO supporters.
    3.4. identifying_yes_no_supporters/kpp_score_yes.csv - Result of running KPP-NEG on yes supporters.
    3.5. identifying_yes_no_supporters/kpp_score_yes.csv - Result of running KPP-NEG on no supporters.
4. spread_of_influence/ -  The folder with data generated for the spread of influence analysis part.
    4.1. spread_of_influence/spread_of_influence_lpa_m.csv - Result of running LPA on M users.
    4.2. spread_of_influence/spread_of_influence_lpa_m2.csv - Result of running LPA on M' users.
    4.3. spread_of_influence/spread_of_influence_lpa_k.csv - Result of running LPA on K users.
    4.4. spread_of_influence/spread_of_influence_modified_lpa_m.csv - Result of running modified LPA on M users.
    4.5. spread_of_influence/spread_of_influence_modified_lpa_2.csv - Result of running modified LPA on M' users.
    4.6. spread_of_influence/spread_of_influence_modified_lpa_k.csv - Result of running LPA on K users.


After running the generate_graphs script each folder will have the graphs used on the report under the figures folder. 


### 6. Report

The report of the analysis with each step taken can be found under Report.pdf.