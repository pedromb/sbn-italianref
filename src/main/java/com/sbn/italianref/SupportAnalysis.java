package com.sbn.italianref;

import com.sbn.italianref.Handlers.CSVHandler;
import com.sbn.italianref.Handlers.GraphHandler;
import com.sbn.italianref.Handlers.TweetsHandler;
import com.sbn.italianref.Models.TweetModel;
import com.sbn.italianref.Models.UserModel;
import it.stilo.g.structures.DoubleValues;
import it.stilo.g.structures.WeightedDirectedGraph;
import it.stilo.g.util.NodesMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.sbn.italianref.Handlers.GraphHandler.kppNegSearchBroker;

public class SupportAnalysis {

    public static List<UserModel> getUsersModel(
            TweetsHandler tw, HashMap<String,
            String> usersMap,
            Path pathToSaveFile
    ) throws IOException {
        System.out.println("Identifying YES/NO Supporters");
        System.out.println();
        System.out.println("[Identifying YES/NO Supporters - Part 1] Part 1 starting!");

        String luceneQuery = "";
        for (String user: usersMap.keySet()) {
            String text = ProcessText.processText(user);
            text = text.equals(user) ? text : text + " || " +user;
            luceneQuery += luceneQuery.length() == 0 ? text : " || "+text;
        }
        luceneQuery += " || iovotosi || bastaunsi || iodicosi || iovotono || bastaunno ||iodicono";
        List<String> fields = new ArrayList<>();
        fields.add("user");
        fields.add("text");
        System.out.print("[Identifying YES/NO Supporters - Part 1] Getting tweets from lucene index...");
        List<TweetModel> result = tw.queryTweets(fields, luceneQuery);
        System.out.println("DONE!");
        System.out.print("[Identifying YES/NO Supporters - Part 1] Processing data and saving file...");
        List<UserModel> users = getUsersAndSaveFile(result, usersMap, pathToSaveFile);
        System.out.println("DONE - File saved in "+pathToSaveFile.toString());
        System.out.println("[Identifying YES/NO Supporters - Part 1] Part 1 DONE!");
        System.out.println();

        return users;
    }


    public static WeightedDirectedGraph centralityAnalysis(
            List<UserModel> users,
            Path graphPath,
            Path usersHitsYesPath,
            Path usersHitsNoPath,
            NodesMapper<String> mapper
    ) throws IOException, InterruptedException {
        System.out.println("[Identifying YES/NO Supporters - Part 2 and 3] Part 2 and 3 starting!");
        Set<String> usersIds = users
                .stream()
                .map(UserModel::getUserId)
                .collect(Collectors.toSet());
        System.out.print(
                "[Identifying YES/NO Supporters - Part 2 and 3] Reading graph from file and generating " +
                        "induced subgraph by users collect in part 1..."
        );
        WeightedDirectedGraph inducedGraph = GraphHandler.readGzGraph(graphPath, usersIds, mapper);
        System.out.println("DONE!");
        System.out.print("[Identifying YES/NO Supporters - Part 2 and 3] " +
                "Finding largest connected component from induced subgraph...");
        WeightedDirectedGraph largestConnectedComponent = GraphHandler.getLargestConnectedComponent(inducedGraph);

        System.out.println("DONE!");
        System.out.print("[Identifying YES/NO Supporters - Part 2 and 3] " +
                "Computing HITS on largest connected component...");
        Map<String, List<Double>> usersHubAuthority = GraphHandler.calculateHits(largestConnectedComponent, mapper);
        List<UserModel> usersFiltered = users.stream().filter(x->usersHubAuthority.containsKey(x.getUserId())).collect(Collectors.toList());

        usersFiltered.stream().forEach(
                x -> {
                    x.setHubScore(usersHubAuthority.get(x.getUserId()).get(1));
                    x.setAuthorityScore(usersHubAuthority.get(x.getUserId()).get(0));
                }
        );
        List<UserModel> usersYes = usersFiltered.stream().filter(x-> x.getSupport().equals("Yes")).collect(Collectors.toList());
        List<UserModel> usersNo = usersFiltered.stream().filter(x-> x.getSupport().equals("No")).collect(Collectors.toList());
        System.out.println("DONE!");
        System.out.print("[Identifying YES/NO Supporters - Part 2 and 3] Calculating metric...");
        double minYesSupportScore = usersYes.stream().min(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double maxYesSupportScore = usersYes.stream().max(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double minYesHubScore = usersYes.stream().min(Comparator.comparing(UserModel::getHubScore)).get().getHubScore();
        double maxYesHubScore = usersYes.stream().max(Comparator.comparing(UserModel::getHubScore)).get().getHubScore();

        double minNoSupportScore = usersNo.stream().max(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double maxNoSupportScore = usersNo.stream().min(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double minNoHubScore = usersNo.stream().min(Comparator.comparing(UserModel::getHubScore)).get().getHubScore();
        double maxNoHubScore = usersNo.stream().max(Comparator.comparing(UserModel::getHubScore)).get().getHubScore();

        usersYes
                .stream()
                .forEach(
                        x-> {
                            double normalizedSupportScore = normalize(x.getSupportScore(), minYesSupportScore, maxYesSupportScore);
                            double normalizedHubScore = normalize(x.getHubScore(), minYesHubScore, maxYesHubScore);
                            x.setCentralityScore(centralityScore(normalizedSupportScore, normalizedHubScore));
                        }
                );

        usersNo
                .stream()
                .forEach(
                        x-> {
                            double normalizedSupportScore = normalize(-x.getSupportScore(), -minNoSupportScore, -maxNoSupportScore);
                            double normalizedHubScore = normalize(x.getHubScore(), minNoHubScore, maxNoHubScore);
                            x.setCentralityScore(centralityScore(normalizedSupportScore, normalizedHubScore));

                        }
                );
        System.out.println("DONE!");
        String [] header = new String[] {"user", "hub_score", "authority_score", "centrality_score"};
        List<String[]> yesRows = new ArrayList<>(){{add(header);}};
        List<String[]> noRows = new ArrayList<>(){{add(header);}};
        DecimalFormat df = new DecimalFormat("###.#####");

        System.out.print("[Identifying YES/NO Supporters - Part 2 and 3] Saving file for YES supporters...");
        for(UserModel user: usersYes) {
            String[] newRow = new String[header.length];
            newRow[0] = user.getUserName();
            newRow[1] = df.format(user.getHubScore());
            newRow[2] = df.format(user.getAuthorityScore());
            newRow[3] = df.format(user.getCentralityScore());
            yesRows.add(newRow);
        }
        CSVHandler.write(usersHitsYesPath, yesRows);
        System.out.println("DONE - File saved in "+usersHitsYesPath.toString());

        System.out.print("[Identifying YES/NO Supporters - Part 2 and 3] Saving file for NO supporters...");
        for(UserModel user: usersNo) {
            String[] newRow = new String[header.length];
            newRow[0] = user.getUserName();
            newRow[1] = df.format(user.getHubScore());
            newRow[2] = df.format(user.getAuthorityScore());
            newRow[3] = df.format(user.getCentralityScore());
            noRows.add(newRow);
        }
        CSVHandler.write(usersHitsNoPath, noRows);
        System.out.println("DONE - File saved in "+usersHitsNoPath.toString());
        System.out.println("[Identifying YES/NO Supporters - Part 2 and 3] Part 2 and 3 DONE!");
        System.out.println();

        return inducedGraph;
    }

    public static void kppNeg(
            List<UserModel> users,
            WeightedDirectedGraph inducedGraph,
            NodesMapper<String> mapper,
            Path kppScoreYesPath,
            Path kppScoreNoPath
    ) throws InterruptedException {

        System.out.println("[Identifying YES/NO Supporters - Part 4] Part 4 starting!");
        WeightedDirectedGraph g = GraphHandler.subsetGraph(inducedGraph, 80 );
        List<Integer> gVertexes = Arrays.stream(g.getVertex()).boxed().collect(Collectors.toList());
        List<UserModel> usersYes = users.stream().filter(x-> x.getSupport().equals("Yes")).collect(Collectors.toList());
        List<UserModel> usersNo = users.stream().filter(x-> x.getSupport().equals("No")).collect(Collectors.toList());

        usersYes = usersYes.stream().filter(x->gVertexes.contains(mapper.getId(x.getUserId()))).collect(Collectors.toList());
        usersNo = usersNo.stream().filter(x->gVertexes.contains(mapper.getId(x.getUserId()))).collect(Collectors.toList());
        int[] usersYesNodes = usersYes
                .stream()
                .mapToInt(x -> mapper.getId(x.getUserId()))
                .toArray();
        int[] usersNoNodes = usersNo
                .stream()
                .mapToInt(x -> mapper.getId(x.getUserId()))
                .toArray();

        String [] header = new String[] {"user", "kpp_score"};
        List<String[]> yesScoresRows = new ArrayList<>() {{add(header);}};
        List<String[]> noScoresRows = new ArrayList<>() {{add(header);}};
        DecimalFormat df = new DecimalFormat("###.#####");

        System.out.println("[Identifying YES/NO Supporters - Part 4] Running KPP-NEG algorithm to find YES brokers...");
        System.out.println();
        List<DoubleValues> brokersYes = kppNegSearchBroker(g, usersYesNodes);
        for(DoubleValues kppScore : brokersYes) {
            String [] newRow = new String[header.length];
            String userId = mapper.getNode(kppScore.index);
            double score = kppScore.value;
            UserModel userModel = usersYes.stream().filter(x->x.getUserId().equals(userId)).findFirst().get();
            newRow[0] = userModel.getUserName();
            newRow[1] = String.valueOf(score);
            yesScoresRows.add(newRow);
        }
        CSVHandler.write(kppScoreYesPath, yesScoresRows);
        System.out.println("[Identifying YES/NO Supporters - Part 4] " +
                "Running KPP-NEG algorithm to find YES brokers...DONE - File saved in "+kppScoreYesPath.toString());

        System.out.println("[Identifying YES/NO Supporters - Part 4] Running KPP-NEG algorithm to find NO brokers...");
        System.out.println();
        List<DoubleValues> brokersNo = kppNegSearchBroker(g, usersNoNodes);
        for(DoubleValues kppScore : brokersNo) {
            String [] newRow = new String[header.length];
            String userId = mapper.getNode(kppScore.index);
            double score = kppScore.value;
            UserModel userModel = usersNo.stream().filter(x->x.getUserId().equals(userId)).findFirst().get();
            newRow[0] = userModel.getUserName();
            newRow[1] = df.format(score);
            noScoresRows.add(newRow);
        }
        CSVHandler.write(kppScoreNoPath, noScoresRows);
        System.out.println("[Identifying YES/NO Supporters - Part 4] " +
                "Running KPP-NEG algorithm to find NO brokers...DONE - File saved in "+kppScoreNoPath.toString());

        System.out.println("[Identifying YES/NO Supporters - Part 4] Part 4 DONE!");
    }



    private static double centralityScore(double normalizedSupportScore, double normalizedHubScore) {
        double metric = (2*normalizedHubScore + normalizedSupportScore) / 3;
        return metric;
    }

    private static List<UserModel> getUsersAndSaveFile(
            List<TweetModel> tweets,
            HashMap<String, String> users,
            Path pathToSaveFile
    ) {
        List<String> yesTokens = new ArrayList<>();
        yesTokens.add("iovotosi");
        yesTokens.add("bastaunsi");
        yesTokens.add("iodicosi");

        List<String> noTokens = new ArrayList<>();
        noTokens.add("iovotono");
        noTokens.add("iodicono");
        noTokens.add("bastaunno");


        List<String> yesUsers = users
                .entrySet()
                .stream()
                .filter(x->x.getValue().equals("yes"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .keySet().stream().collect(Collectors.toList());
        List<String> noUsers = users
                .entrySet()
                .stream()
                .filter(x->x.getValue().equals("no"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .keySet().stream().collect(Collectors.toList());

        Map<String, List<TweetModel>> tweetsByUser = tweets.stream()
                .collect(Collectors.groupingBy(TweetModel::getUser));
        List<UserModel> res = new ArrayList<>();
        List<String[]> rows = new ArrayList<>();
        String [] header = new String[]{"user", "support", "number_of_tweets"};
        rows.add(header);
        for(String user : tweetsByUser.keySet()) {
            double supportScore = getSupportScore(tweetsByUser.get(user), yesUsers, noUsers, yesTokens, noTokens);
            UserModel newUser = new UserModel();
            TweetModel firstTweet = tweetsByUser.get(user).get(0);
            newUser.setUserName(user);
            newUser.setUserId(firstTweet.getUserId());
            newUser.setSupportScore(supportScore);
            res.add(newUser);
            String[] newRow = new String[header.length];
            newRow[0] = newUser.getUserName();
            newRow[1] = newUser.getSupport();
            newRow[2] = String.valueOf(tweetsByUser.get(user).size());
            rows.add(newRow);
        }
        CSVHandler.write(pathToSaveFile, rows);
        return res;
    }

    private static double getSupportScore(
            List<TweetModel> userTweets,
            List<String> yesUsersSupport,
            List<String> noUsersSupport,
            List<String> yesTokens,
            List<String> noTokens
    ) {
        List<String> processedNoUsers = noUsersSupport.stream().map(ProcessText::processText).collect(Collectors.toList());
        List<String> processedYesUsers = yesUsersSupport.stream().map(ProcessText::processText).collect(Collectors.toList());
        String userName = userTweets.get(0).getUser();
        double noScore = 0;
        double yesScore = 0;
        if(noUsersSupport.contains(userName)) noScore =  -1.0;
        if(yesUsersSupport.contains(userName)) yesScore = 1.0;
        for(TweetModel userTweet : userTweets) {
            Map<String, Long> termsVector = userTweet.getTermsVector();
            for(String term : termsVector.keySet()) {
                double termFreq = termsVector.get(term);
                if(processedNoUsers.contains(term)) noScore -= termFreq;
                if(processedYesUsers.contains(term)) yesScore += termFreq;
                if(yesTokens.contains(term)) yesScore += 3*termFreq;
                if(noTokens.contains(term)) noScore -= 3*termFreq;
            }
        }
        double scoreSum = yesScore + noScore;
        return scoreSum;
    }

    private static double normalize(double value, double min, double max) {
        return ((value - min) / (max - min));
    }

}
