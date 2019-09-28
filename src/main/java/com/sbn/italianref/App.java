package com.sbn.italianref;


import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.IndexReader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class App {

    private static String STREAM_RESOURCE = "stream";
    private static String USERS_RESOURCE = "users.csv";
    private static String LUCENE_INDEX = "lucene_index/";
    private static String TEMPORAL_ANALYSIS_DISTRIBUTION_CSV = "tweets_distribution.csv";
    private static String CLUSTERS_YES_CSV = "clusters_yes.csv";
    private static String CLUSTERS_NO_CSV = "clusters_no.csv";
    private static String GRAPHS_FOLDER = "graphs/";
    private static String USERS_GRAPH_RESOURCE = "Official_SBN-ITA-2016-Net.gz";


    private static boolean INCLUDE_RETWEETS = false;

    private static int TWEETS_LIMIT =   0;

    public static void main(String[] args) throws Exception {
        ResourcesHandler rh = new ResourcesHandler();
        Path streamPath = rh.getDirFromResources(STREAM_RESOURCE);
        Path usersPath = rh.getDirFromResources(USERS_RESOURCE);
        Path usersGraphPath = rh.getDirFromResources(USERS_GRAPH_RESOURCE);

        Path luceneIndexPath = Paths.get(LUCENE_INDEX);
        Path temporalAnalysisDistributionCsv = Paths.get(TEMPORAL_ANALYSIS_DISTRIBUTION_CSV);
        Path clustersYesCsv = Paths.get(CLUSTERS_YES_CSV);
        Path clustersNoCsv = Paths.get(CLUSTERS_NO_CSV);
        Path graphsFolder = Paths.get(GRAPHS_FOLDER);


        LuceneWrapper.luceneIndexPath = luceneIndexPath;
        TweetsWrapper tw = new TweetsWrapper(streamPath, TWEETS_LIMIT);
        if(Files.notExists(luceneIndexPath)) {
            tw.indexTweetsToLucene();
        }

        List<String[]> users = CSVHandler.read(usersPath, true);
        HashMap<String, String> usersMap = (HashMap<String, String>) users
                .stream()
                .collect(
                        Collectors.toMap(
                                user -> (String) Arrays.asList(user).get(0),
                                user -> (String) Arrays.asList(user).get(1)
                        )
                );

        List<UserModel> usersModel = GraphAnalysis.getUsersModel(tw, usersMap);
        GraphAnalysis.centralityAnalysis(usersModel, usersGraphPath);
       /* List<TweetModel> tweets = TemporalAnalysis.influencersSupportAnalysis(
                tw, usersMap,
                temporalAnalysisDistributionCsv, INCLUDE_RETWEETS
        );
        Map<String, Map<String, List<String>>> clustersBySupport = TemporalAnalysis.clusterTerms(
                tw,  usersMap,  tweets, INCLUDE_RETWEETS,
                12, 20, 3,
                100, clustersYesCsv,  clustersNoCsv
        );
        TemporalAnalysis.coreComponents(clustersBySupport, tweets, graphsFolder);
        TemporalAnalysis.coreComponentsTimeseris(tw, tweets, 3);*/

    }
}