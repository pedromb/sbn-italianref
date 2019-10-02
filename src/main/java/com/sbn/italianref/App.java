package com.sbn.italianref;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.sbn.italianref.Handlers.CSVHandler;
import com.sbn.italianref.Handlers.LuceneHandler;
import com.sbn.italianref.Handlers.ResourcesHandler;
import com.sbn.italianref.Handlers.TweetsHandler;
import com.sbn.italianref.Models.TweetModel;
import com.sbn.italianref.Models.UserModel;
import it.stilo.g.structures.WeightedDirectedGraph;
import it.stilo.g.util.NodesMapper;


public class App {

    private static String STREAM_RESOURCE = "stream";
    private static String USERS_RESOURCE = "users.csv";
    private static String LUCENE_INDEX = "lucene_index/";
    private static String TEMPORAL_ANALYSIS_FOLDER = "temporal_analysis/";
    private static String IDENTIFYING_YES_NO_SUPPORTERS_FOLDER = "identifying_yes_no_supporters/";
    private static String SPREAD_OF_INFLUENCE_FOLDER = "spread_of_influence/";
    private static String TEMPORAL_ANALYSIS_DISTRIBUTION_CSV = TEMPORAL_ANALYSIS_FOLDER+"tweets_distribution.csv";
    private static String CLUSTERS_CSV = TEMPORAL_ANALYSIS_FOLDER+"clusters.csv";
    private static String GRAPHS_FOLDER = TEMPORAL_ANALYSIS_FOLDER+"graphs/";
    private static String K_CORE_TS_YES = TEMPORAL_ANALYSIS_FOLDER+"k_core_timeseries_yes.csv";
    private static String K_CORE_TS_NO = TEMPORAL_ANALYSIS_FOLDER+"k_core_timeseries_no.csv";
    private static String USERS_SUPPORT_CSV = IDENTIFYING_YES_NO_SUPPORTERS_FOLDER + "users_support.csv";
    private static String USERS_HITS_YES_CSV = IDENTIFYING_YES_NO_SUPPORTERS_FOLDER + "users_hits_yes.csv";
    private static String USERS_HITS_NO_CSV = IDENTIFYING_YES_NO_SUPPORTERS_FOLDER + "users_hits_no.csv";
    private static String KPP_SCORE_YES_CSV = IDENTIFYING_YES_NO_SUPPORTERS_FOLDER + "kpp_score_yes.csv";
    private static String KPP_SCORE_NO_CSV = IDENTIFYING_YES_NO_SUPPORTERS_FOLDER + "kpp_score_no.csv";
    private static String SPREAD_OF_INFLUENCE_M_CSV = SPREAD_OF_INFLUENCE_FOLDER + "spread_of_influence_m.csv";
    private static String SPREAD_OF_INFLUENCE_M2_CSV = SPREAD_OF_INFLUENCE_FOLDER + "spread_of_influence_m2.csv";
    private static String SPREAD_OF_INFLUENCE_K_CSV = SPREAD_OF_INFLUENCE_FOLDER + "spread_of_influence_k.csv";



    private static String USERS_GRAPH_RESOURCE = "Official_SBN-ITA-2016-Net.gz";
    private static int TWEETS_LIMIT =   0;

    public static void main(String[] args) throws Exception {
        Path temporalAnalysisPath =  Paths.get(TEMPORAL_ANALYSIS_FOLDER);
        Path identifyingYesNoSupportersPath =  Paths.get(IDENTIFYING_YES_NO_SUPPORTERS_FOLDER);
        Path spreadOfInfluencePath =  Paths.get(SPREAD_OF_INFLUENCE_FOLDER);

        createFolder(temporalAnalysisPath);
        createFolder(identifyingYesNoSupportersPath);
        createFolder(spreadOfInfluencePath);

        ResourcesHandler rh = new ResourcesHandler();
        Path streamPath = rh.getDirFromResources(STREAM_RESOURCE);
        Path usersPath = rh.getDirFromResources(USERS_RESOURCE);
        Path usersGraphPath = rh.getDirFromResources(USERS_GRAPH_RESOURCE);

        Path luceneIndexPath = Paths.get(LUCENE_INDEX);
        Path temporalAnalysisDistributionCsv = Paths.get(TEMPORAL_ANALYSIS_DISTRIBUTION_CSV);
        Path clustersCsv = Paths.get(CLUSTERS_CSV);
        Path graphsFolder = Paths.get(GRAPHS_FOLDER);
        Path kCoreTsYesCsv = Paths.get(K_CORE_TS_YES);
        Path kCoreTsNoCsv = Paths.get(K_CORE_TS_NO);
        Path usersSupportCsv = Paths.get(USERS_SUPPORT_CSV);
        Path usersHitsYesCsv = Paths.get(USERS_HITS_YES_CSV);
        Path usersHitsNoCsv = Paths.get(USERS_HITS_NO_CSV);
        Path kppScoreYesCsv = Paths.get(KPP_SCORE_YES_CSV);
        Path kppScoresNoCsv = Paths.get(KPP_SCORE_NO_CSV);

        Path spreadOfInfluenceM = Paths.get(SPREAD_OF_INFLUENCE_M_CSV);
        Path spreadOfInfluenceM2 = Paths.get(SPREAD_OF_INFLUENCE_M2_CSV);
        Path spreadOfInfluenceK = Paths.get(SPREAD_OF_INFLUENCE_K_CSV);

        LuceneHandler.luceneIndexPath = luceneIndexPath;
        TweetsHandler tw = new TweetsHandler(streamPath, TWEETS_LIMIT);
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

        List<TweetModel> tweets = TemporalAnalysis.getUsersSupport(
                tw, usersMap, temporalAnalysisDistributionCsv
        );
        Map<String, Map<String, List<String>>> clustersBySupport = TemporalAnalysis.saxClusters(
                tw, tweets, 12, 20, 3, 100, clustersCsv
        );

        TemporalAnalysis.coreComponents(clustersBySupport, tweets, graphsFolder);
        TemporalAnalysis.coreComponentsTimeseris(tw, tweets, 3, kCoreTsYesCsv, kCoreTsNoCsv);
        NodesMapper<String> supportAnalysisGraphMapper = new NodesMapper<>();
        List<UserModel> usersModel = SupportAnalysis.getUsersModel(tw, usersMap, usersSupportCsv);
        WeightedDirectedGraph inducedGraph = SupportAnalysis.centralityAnalysis(
                usersModel, usersGraphPath, usersHitsYesCsv, usersHitsNoCsv, supportAnalysisGraphMapper
        );
/*
        SupportAnalysis.kppNeg(usersModel, inducedGraph, supportAnalysisGraphMapper, kppScoreYesCsv, kppScoresNoCsv);
*/
        SpreadOfInfluence.run(
                inducedGraph, supportAnalysisGraphMapper, usersModel, 100,
                spreadOfInfluenceK, spreadOfInfluenceM, spreadOfInfluenceM2
        );

    }

    private static void createFolder(Path folder) {
        if (!Files.exists(folder)) {
            try {
                Files.createDirectories(folder);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}