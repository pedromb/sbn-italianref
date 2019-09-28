package com.sbn.italianref;

import it.stilo.g.algo.HubnessAuthority;
import it.stilo.g.structures.DoubleValues;
import it.stilo.g.structures.WeightedUndirectedGraph;
import it.stilo.g.util.NodesMapper;
import twitter4j.User;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class GraphAnalysis {

    public static List<UserModel> getUsersModel(TweetsWrapper tw, HashMap<String, String> usersMap) throws IOException {
        System.out.println("#################### Identifying YES/NO Supporters - Part 1 ####################");
        System.out.println();
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
        List<TweetModel> result = tw.queryTweets(fields, luceneQuery);
        List<UserModel> users = getUsersAndPrintStats(result, usersMap);
        users = users.stream().filter(x -> x.getSupportScore() != 0).collect(Collectors.toList());
        System.out.println("#################### End of Identifying YES/NO Supporters - Part 1 ####################");
        return users;
    }


    public static WeightedUndirectedGraph centralityAnalysis(List<UserModel> users, Path graphPath) throws IOException, InterruptedException {
        Set<String> usersIds = users
                .stream()
                .map(UserModel::getUserId)
                .collect(Collectors.toSet());
        NodesMapper<String> mapper = new NodesMapper<>();
        WeightedUndirectedGraph inducedGraph = GraphWrapper.readGzGraph(graphPath, usersIds, mapper);
        WeightedUndirectedGraph largestConnectedComponent = GraphWrapper.getLargestConnectedComponent(inducedGraph);
        Map<String, Double> usersHubAuthority = GraphWrapper.calculateHubnessAuthority(largestConnectedComponent);
        users = users.stream().filter(x->usersHubAuthority.containsKey(x.getUserId())).collect(Collectors.toList());
        users.stream().forEach(x -> x.setHubnessAuthority(usersHubAuthority.get(x.getUserId())));
        List<UserModel> usersYes = users.stream().filter(x-> x.getSupportScore() > 0).collect(Collectors.toList());
        List<UserModel> usersNo = users.stream().filter(x-> x.getSupportScore() < 0).collect(Collectors.toList());

        double minYesSupportScore = usersYes.stream().min(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double maxYesSupportScore = usersYes.stream().max(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double minYesHubScore = usersYes.stream().min(Comparator.comparing(UserModel::getHubnessAuthority)).get().getHubnessAuthority();
        double maxYesHubScore = usersYes.stream().max(Comparator.comparing(UserModel::getHubnessAuthority)).get().getHubnessAuthority();

        double minNoSupportScore = usersNo.stream().max(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double maxNoSupportScore = usersNo.stream().min(Comparator.comparing(UserModel::getSupportScore)).get().getSupportScore();
        double minNoHubScore = usersNo.stream().min(Comparator.comparing(UserModel::getHubnessAuthority)).get().getHubnessAuthority();
        double maxNoHubScore = usersNo.stream().max(Comparator.comparing(UserModel::getHubnessAuthority)).get().getHubnessAuthority();


        usersYes
                .stream()
                .forEach(
                        x-> {
                            double normalizedSupportScore = normalize(x.getSupportScore(), minYesSupportScore, maxYesSupportScore);
                            double normalizedHubScore = normalize(x.getHubnessAuthority(), minYesHubScore, maxYesHubScore);
                            x.setCentralityScore(centralityScore(normalizedSupportScore, normalizedHubScore));

                        }
                );

        usersNo
                .stream()
                .forEach(
                        x-> {
                            double normalizedSupportScore = normalize(-x.getSupportScore(), -minNoSupportScore, -maxNoSupportScore);
                            double normalizedHubScore = normalize(x.getHubnessAuthority(), minNoHubScore, maxNoHubScore);
                            x.setCentralityScore(centralityScore(normalizedSupportScore, normalizedHubScore));

                        }
                );
        System.out.println();
        System.out.println("Top 10 by Centrality Score - Yes");
        System.out.println();
        usersYes
                .stream()
                .sorted(Collections.reverseOrder(Comparator.comparing(UserModel::getCentralityScore)))
                .limit(10)
                .forEach(x -> System.out.println(x.getUserName()+": "+x.getCentralityScore()));
        System.out.println();
        System.out.println("Top 10 by Centrality Score - No");
        System.out.println();
        usersNo
                .stream()
                .sorted(Collections.reverseOrder(Comparator.comparing(UserModel::getCentralityScore)))
                .limit(10)
                .forEach(x -> System.out.println(x.getUserName()+": "+x.getCentralityScore()));
        System.out.println();

        return inducedGraph;
    }

    private static double centralityScore(double normalizedSupportScore, double normalizedHubScore) {
        double metric = (2*normalizedHubScore + normalizedSupportScore) / 3;
        return metric;
    }

    private static List<UserModel> getUsersAndPrintStats(List<TweetModel> tweets, HashMap<String, String> users) {
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
        for(String user : tweetsByUser.keySet()) {
            double supportScore = getSupportScore(tweetsByUser.get(user), yesUsers, noUsers, yesTokens, noTokens);
            UserModel newUser = new UserModel();
            TweetModel firstTweet = tweetsByUser.get(user).get(0);
            newUser.setUserName(user);
            newUser.setUserId(firstTweet.getUserId());
            newUser.setSupportScore(supportScore);
            res.add(newUser);
        }

        Map<String, Long> numUsersBySupport = res
                .stream()
                .collect(
                        Collectors.groupingBy(UserModel::getSupport,
                        Collectors.counting()
                ));
        System.out.println("#################### Number of Users ####################");
        System.out.println();
        System.out.println("Number of Users - Total: "+tweetsByUser.size());
        for (String support: numUsersBySupport.keySet()) {
            System.out.println("Number of Users - "+ support +": "+numUsersBySupport.get(support));
        }
        System.out.println();
        System.out.println("#################### Number of Tweets ####################");
        System.out.println();

        Map<String, String> usersAndSupport = res.stream().collect(Collectors.toMap(x->x.getUserName(), x->x.getSupport()));
        Map<String, List<TweetModel>> tweetsBySupport = tweets.stream()
                .collect(
                        Collectors.groupingBy(
                                x->usersAndSupport.get(x.getUser())
                        ));
        System.out.println("Number of Tweets - Total: "+tweets.size());
        for (String support: tweetsBySupport.keySet()) {
            System.out.println("Number of Tweets - "+ support +": "+tweetsBySupport.get(support).size());

        }
        System.out.println();

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
                if(yesTokens.contains(term)) yesScore += 1.5*termFreq;
                if(noTokens.contains(term)) noScore -= 1.5*termFreq;
            }
        }
        double scoreSum = yesScore + noScore;
        return scoreSum;
    }

    private static double normalize(double value, double min, double max) {
        return ((value - min) / (max - min));
    }

}
