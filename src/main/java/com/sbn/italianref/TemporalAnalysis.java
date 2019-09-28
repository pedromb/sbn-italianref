package com.sbn.italianref;

import it.stilo.g.structures.WeightedUndirectedGraph;
import it.stilo.g.util.NodesMapper;
import net.seninp.jmotif.sax.SAXException;
import net.seninp.jmotif.sax.SAXProcessor;
import net.seninp.jmotif.sax.alphabet.NormalAlphabet;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class TemporalAnalysis {

    public static List<TweetModel> influencersSupportAnalysis(
            TweetsWrapper tweetsWrapper,
            HashMap<String, String> users,
            Path distributionFilename,
            boolean includeRetweets
    ) throws IOException {

        System.out.println("");
        System.out.println("#################### Temporal Analysis - Part 1 ####################");
        String luceneQuery = "";
        for(String user : users.keySet()) {
            if(luceneQuery.length() == 0) {
                luceneQuery += user;
            }
            else {
                luceneQuery += " || "+user;
            }
        }
        String field = includeRetweets ? "user_created_tweet" : "user";
        List<String> fields = new ArrayList<>();
        fields.add(field);
        List<TweetModel> tweets = tweetsWrapper.queryTweets(fields, luceneQuery);
        tweets = tweetsWrapper.addSupportToTweetModel(tweets, users);
        printNumberOfUsersBySupport(tweets);
        printNumberOfTweets(tweets, includeRetweets);
        saveDistributionList(tweets, distributionFilename);
        System.out.println("");
        System.out.println("################ End of Temporal Analysis - Part 1  #################");
        return tweets;
    }

    public static Map<String, Map<String, List<String>>> clusterTerms(
            TweetsWrapper tweetsWrapper,
            HashMap<String, String> users,
            List<TweetModel> tweets,
            boolean includeRetweets,
            int timeWindow,
            int alphabetSize,
            int k,
            int maxIterations,
            Path clustersYesPath,
            Path clustersNoPath
    ) throws IOException, SAXException {
        System.out.println("");
        System.out.println("#################### Temporal Analysis - Part 2 ####################");
        System.out.println("");
        String luceneQueryYes = "";
        String luceneQueryNo = "";

        for(String user : users.keySet()) {
            if(users.get(user).equals("yes")) {
                if(luceneQueryYes.length() == 0) {
                    luceneQueryYes += user;
                }
                else {
                    luceneQueryYes += " | "+user;
                }
            } else {
                if(luceneQueryNo.length() == 0) {
                    luceneQueryNo += user;
                }
                else {
                    luceneQueryNo += " | "+user;
                }
            }

        }
        String field = includeRetweets ? "user_created_tweet" : "user";
        NormalAlphabet na = new NormalAlphabet();
        double[][] distanceMatrix = na.getDistanceMatrix(alphabetSize);

        List<TweetModel> tweetsYes = tweets.stream()
                .filter(x -> x.getActualSupport().equals("yes"))
                .collect(Collectors.toList());
        List<TweetModel> tweetsNo = tweets.stream()
                .filter(x -> x.getActualSupport().equals("no"))
                .collect(Collectors.toList());


        Map<String, Long> termsFreqYes = tweetsWrapper.getTermsFrequency(field, luceneQueryYes, 1000);
        Map<String, TermsTimeSeries> timeseriesYes = getTermsTimeseries(termsFreqYes, tweetsYes, timeWindow);
        timeseriesYes = addSax(timeseriesYes, alphabetSize, timeWindow);
        System.out.println("#################### Top 5 Words - Yes Support ####################");
        System.out.println("");
        termsFreqYes.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(5)
                .forEach(x -> {
                    System.out.println(x.getKey() + ": "+x.getValue() + " ");
                });
        System.out.println("");

        Map<String, Long> termsFreqNo = tweetsWrapper.getTermsFrequency(field, luceneQueryNo, 1000);
        Map<String, TermsTimeSeries> timeseriesNo = getTermsTimeseries(termsFreqNo, tweetsNo, timeWindow);
        timeseriesNo = addSax(timeseriesNo, alphabetSize, timeWindow);
        System.out.println("#################### Top 5 Words - No Support ####################");
        System.out.println("");
        termsFreqNo.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(5)
                .forEach(x -> {
                    System.out.println(x.getKey() + ": "+x.getValue());
                });
        System.out.println("");

        System.out.println("#################### Running KMeans - Yes Support ####################");
        Map<String, List<String>> clustersYes = KMeans.fitForSax(timeseriesYes, k, distanceMatrix, alphabetSize, maxIterations);
        List<String[]> clustersRowsYes = createClusterLists(clustersYes, termsFreqYes, timeseriesYes);
        Map<Integer, List<String>> topWordsByClusterYes = getTopWordsByCluster(clustersYes, termsFreqYes, 10);
        topWordsByClusterYes.entrySet()
                .stream()
                .forEach(x -> {
                    System.out.println("Cluster "+x.getKey()+": "+x.getValue());
                });
        CSVHandler.write(clustersYesPath, clustersRowsYes);
        System.out.println("");

        System.out.println("#################### Running KMeans - No Support ####################");
        Map<String, List<String>> clustersNo = KMeans.fitForSax(timeseriesNo, k, distanceMatrix, alphabetSize, maxIterations);
        List<String[]> clustersRowsNo = createClusterLists(clustersNo, termsFreqNo, timeseriesNo);
        Map<Integer, List<String>> topWordsByClusterNo = getTopWordsByCluster(clustersNo, termsFreqNo, 10);
        topWordsByClusterNo.entrySet()
                .stream()
                .forEach(x -> {
                    System.out.println("Cluster "+x.getKey()+": "+x.getValue());
                });
        System.out.println("");
        CSVHandler.write(clustersNoPath, clustersRowsNo);
        System.out.println("");
        System.out.println("################ End of Temporal Analysis - Part 2  #################");
        System.out.println("");

        Map<String, Map<String, List<String>>> clustersBySupport = new HashMap<>();
        clustersBySupport.put("yes", clustersYes);
        clustersBySupport.put("no", clustersNo);
        return clustersBySupport;

    }

    public static void coreComponents(
            Map<String, Map<String, List<String>>> clusterBySupport,
            List<TweetModel> tweets,
            Path graphsPath
    ) throws IOException, ParseException, InterruptedException {

        Map<String, List<String>> yesClusters = clusterBySupport.get("yes");
        Map<String, List<String>> noClusters = clusterBySupport.get("no");
        List<TweetModel> tweetsYes = tweets.stream()
                .filter(x -> x.getActualSupport().equals("yes"))
                .collect(Collectors.toList());

        List<TweetModel> tweetsNo = tweets.stream()
                .filter(x -> x.getActualSupport().equals("no"))
                .collect(Collectors.toList());

        Map<String, Set<Integer>> yesTweetsTermsInverseIndex = getTweetsTermInverseIndex(tweetsYes);
        Map<String, Set<Integer>> noTweetsTermsInverseIndex = getTweetsTermInverseIndex(tweetsNo);

        Path yesGraphsPath = Paths.get(graphsPath.toString() + "/yes/");
        Path noGraphsPath = Paths.get(graphsPath.toString() + "/no/");
        if (!Files.exists(yesGraphsPath)) {
            try {
                Files.createDirectories(yesGraphsPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!Files.exists(noGraphsPath)) {
            try {
                Files.createDirectories(noGraphsPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        int clusterId = 1;
        for(List<String> clusterTerms: yesClusters.values()) {
            processGraphs(clusterTerms, yesTweetsTermsInverseIndex, 0.1, yesGraphsPath, clusterId);
            clusterId += 1;
        }
        clusterId = 1;
        for(List<String> clusterTerms: noClusters.values()) {
            processGraphs(clusterTerms, noTweetsTermsInverseIndex, 0.1, noGraphsPath, clusterId);
            clusterId += 1;
        }


    }

    public static void coreComponentsTimeseris(
            TweetsWrapper tw,
            List<TweetModel> tweets,
            int k
    ) throws IOException {
        List<TweetModel> tweetsYes = tweets.stream()
                .filter(x -> x.getActualSupport().equals("yes"))
                .collect(Collectors.toList());

        List<TweetModel> tweetsNo = tweets.stream()
                .filter(x -> x.getActualSupport().equals("no"))
                .collect(Collectors.toList());

        Map<String, Long> yesTermsFreq = tw.getTermsFrequencyByDocIds(
                tweetsYes.stream().mapToInt(x -> x.getDocId()).toArray()
        );
        Map<String, Long> noTermsFreq = tw.getTermsFrequencyByDocIds(
                tweetsNo.stream().mapToInt(x -> x.getDocId()).toArray()
        );

        List<String[]> yesRows = new ArrayList<>();
        List<String[]> noRows = new ArrayList<>();
        String[] header = new String[4];
        header[0] = "term";
        header[1] = "graph_k_core";
        header[2] = "date";
        header[3] = "number_of_tweets";
        yesRows.add(header);
        noRows.add(header);

        for(int i = 0; i<k; i++) {
            int clusterId = i+1;
            List<String> yesComponents = getGraphComponents("yes", clusterId);
            Map<String, TermsTimeSeries> timeseriesYes = getTermsTimeseries(yesTermsFreq, tweetsYes, 3);
            timeseriesYes = timeseriesYes.entrySet()
                    .stream()
                    .filter(x -> yesComponents.contains(x.getValue().term))
                    .collect(Collectors.toMap(x -> x.getKey(), x-> x.getValue()));
            List<String[]> yesNewRows = getGraphRows(timeseriesYes, clusterId);
            yesRows.addAll(yesNewRows);

            List<String> noComponents = getGraphComponents("no", clusterId);
            Map<String, TermsTimeSeries> timeseriesNo = getTermsTimeseries(noTermsFreq, tweetsNo, 3);
            timeseriesNo = timeseriesNo.entrySet()
                    .stream()
                    .filter(x -> noComponents.contains(x.getValue().term))
                    .collect(Collectors.toMap(x -> x.getKey(), x-> x.getValue()));
            List<String[]> noNewsRo = getGraphRows(timeseriesNo, clusterId);
            noRows.addAll(noNewsRo);
        }
        Path yesPath = Paths.get("graphs_yes.csv");
        CSVHandler.write(yesPath, yesRows);
        Path noPath = Paths.get("graphs_no.csv");
        CSVHandler.write(noPath, noRows);

    }

    private static List<String[]> getGraphRows(Map<String, TermsTimeSeries> timeseries, int clusterId) {
        List<String[]> rows= new ArrayList<String[]>();
        String cluster = String.valueOf(clusterId);
        for(String term : timeseries.keySet()) {
            TermsTimeSeries termTs = timeseries.get(term);
            if(termTs != null) {
                double[] ts = timeseries.get(term).getCompressedTs();
                LocalDateTime[] dates = timeseries.get(term).getCompressedTsDates();
                for (int i = 0; i < ts.length; i++) {
                    String[] newEntry = new String[4];
                    newEntry[0] = term;
                    newEntry[1] = cluster;
                    newEntry[2] = String.valueOf(dates[i]);
                    newEntry[3] = String.valueOf(ts[i]);
                    rows.add(newEntry);
                }
            }
        }
        return rows;
    }

    private static void processGraphs(
            List<String> clusterTerms,  Map<String, Set<Integer>> tweetsTermsInverseIndex,
            double threshold, Path graphsPath, int clusterId) throws IOException, InterruptedException {

        String fullGraphFolder = graphsPath.toString() + "/full_graph_" + clusterId + ".txt";
        String largestCCFolder = graphsPath.toString() + "/largest_cc_graph_" + clusterId + ".txt";
        String kCoreFolder = graphsPath.toString() + "/k_core_graph_" + clusterId + ".txt";

        NodesMapper<String> mapper = new NodesMapper<>();
        WeightedUndirectedGraph yesGraph = GraphWrapper.createCoGraph(
                clusterTerms, tweetsTermsInverseIndex, mapper, threshold
        );
        WeightedUndirectedGraph yesCCGraph = GraphWrapper.getLargestConnectedComponent(yesGraph);
        WeightedUndirectedGraph yesKCoreGraph = GraphWrapper.getKCore(yesGraph);
        Path fullGraphPath = Paths.get(fullGraphFolder);
        Path largestCCPath = Paths.get(largestCCFolder);
        Path kCorePath = Paths.get(kCoreFolder);
        Set<String> fullGraphComponents = GraphWrapper.saveGraph(yesGraph, fullGraphPath);
        Set<String> coreComponentComponents = GraphWrapper.saveGraph(yesCCGraph, largestCCPath);
        Set<String> kCoreComponents = GraphWrapper.saveGraph(yesKCoreGraph, kCorePath);

        String fullGraphComponentsFolder = graphsPath.toString() + "/full_graph_components" + clusterId + ".txt";
        String largestCCComponentsFolder = graphsPath.toString() + "/largest_cc_graph_components" + clusterId + ".txt";
        String kCoreComponentsFolder = graphsPath.toString() + "/k_core_graph_components" + clusterId + ".txt";
        Path fullGraphComponentsPath = Paths.get(fullGraphComponentsFolder);
        Path largestCCComponentsPath = Paths.get(largestCCComponentsFolder);
        Path kCoreComponentsPath = Paths.get(kCoreComponentsFolder);

        Files.write(fullGraphComponentsPath, fullGraphComponents, StandardCharsets.UTF_8);
        Files.write(largestCCComponentsPath, coreComponentComponents, StandardCharsets.UTF_8);
        Files.write(kCoreComponentsPath, kCoreComponents, StandardCharsets.UTF_8);

    }

    private static Map<String, TermsTimeSeries> getTermsTimeseries(Map<String, Long> terms, List<TweetModel> tweets, int timeWindow) {
        LocalDateTime maxDate =  Collections
                .max(tweets, Comparator.comparing(t -> t.getCreatedAtTruncatedHours()))
                .getCreatedAtTruncatedHours();
        LocalDateTime minDate =  Collections
                .min(tweets, Comparator.comparing(t -> t.getCreatedAtTruncatedHours()))
                .getCreatedAtTruncatedHours();
        long hours = minDate.until( maxDate, ChronoUnit.HOURS);
        ArrayList<LocalDateTime> intervals = new ArrayList<LocalDateTime>();
        intervals.add(minDate);
        while(minDate.isBefore(maxDate)) {
            minDate = minDate.plusHours(1);
            intervals.add(minDate);
        }
        Map<String, TermsTimeSeries> termsTimeSeries = new HashMap<String, TermsTimeSeries>();
        for(TweetModel tweet : tweets) {
            Map<String, Long> termsVector = tweet.getTermsVector();
            for(String term : termsVector.keySet()) {
                if(terms.containsKey(term)) {
                    LocalDateTime termDate = tweet.getCreatedAtTruncatedHours();
                    int position = intervals.indexOf(termDate);
                    long termFreq = termsVector.get(term);
                    if(termsTimeSeries.containsKey(term)) {
                        TermsTimeSeries ts = termsTimeSeries.get(term);
                        ts.addToOriginalTs(termFreq, position);
                    } else {
                        double[] ts = new double[intervals.size()];
                        Arrays.fill(ts, 0);
                        ts[position] = termFreq;
                        TermsTimeSeries newTs = new TermsTimeSeries();
                        newTs.setTerm(term);
                        newTs.setOriginalTs(ts);
                        termsTimeSeries.put(term, newTs);
                    }
                }
            }

        }

        for(String term : termsTimeSeries.keySet()) {
            double [] originalTs = termsTimeSeries.get(term).getOriginalTs();
            double [] compressedTs = compressTimeSeries(originalTs, 3);
            LocalDateTime [] termDates = intervals.stream().toArray(LocalDateTime[]::new);
            LocalDateTime [] compresedTermDates = compressTermDates(termDates, 3);
            termsTimeSeries.get(term).setCompressedTs(compressedTs);
            termsTimeSeries.get(term).setCompressedTsDates(compresedTermDates);
            termsTimeSeries.get(term).setOriginalTsDates(termDates);
        }

        return termsTimeSeries;
    }

    private static List<String[]> createDistributionList(List<TweetModel> tweets) {
        String[] header = new String[]{"date", "support", "number_of_tweets"};
        List <String[]> rows = new ArrayList<String[]>();
        rows.add(header);
        Map<LocalDateTime, Map<String, Long>> distribution = tweets
                .stream()
                .collect(
                        Collectors.groupingBy(x -> x.getCreatedAtTruncatedHours(),
                                Collectors.groupingBy(TweetModel::getActualSupport, Collectors.counting()))
                );
        TreeMap<LocalDateTime, Map<String, Long>> sortedDistribution = new TreeMap<>(distribution);
        for(LocalDateTime dateTime: sortedDistribution.keySet()) {
            String[] newRowYes = new String[3];
            String[] newRowNo = new String[3];
            String date = dateTime.toString();
            String supportYesNTweets = sortedDistribution.get(dateTime).containsKey("yes") ?
                    sortedDistribution.get(dateTime).get("yes").toString() : "0";
            String supportNoNTweets =  sortedDistribution.get(dateTime).containsKey("no") ?
                    sortedDistribution.get(dateTime).get("no").toString() : "0";
            newRowYes[0] = date;
            newRowYes[1] = "yes";
            newRowYes[2] = supportYesNTweets;
            newRowNo[0] = date;
            newRowNo[1] = "no";
            newRowNo[2] = supportNoNTweets;
            rows.add(newRowNo);
            rows.add(newRowYes);
        }
        return rows;
    }

    private static void printNumberOfUsersBySupport(List<TweetModel> tweets) {
        Map<String, Integer> usersBySupport = tweets
                .stream()
                .collect(Collectors.groupingBy(TweetModel::getUserSupport,
                        Collectors.collectingAndThen(Collectors.mapping(
                                TweetModel::getUser, Collectors.toSet()),
                                Set::size)));
        System.out.println("");
        System.out.println("########## Number of Users by Support ##########\n");
        System.out.println("Yes: " + usersBySupport.get("yes"));
        System.out.println("No: " + usersBySupport.get("no"));
        System.out.println("\n################################################");
    }

    private static void printNumberOfTweets(List<TweetModel> tweets, boolean includeRetweets) {

        Map<String, Long> usersBySupport = tweets
                .stream()
                .collect(
                        Collectors.groupingBy(
                                x -> includeRetweets ? x.getActualSupport() : x.getUserSupport(),
                                Collectors.counting()
                        )
                );
        System.out.println("");
        System.out.println("########## Number of Tweets by Support ##########\n");
        System.out.println("Yes: " + usersBySupport.get("yes"));
        System.out.println("No: " + usersBySupport.get("no"));
        System.out.println("\n#################################################");
    }

    private static void saveDistributionList(List<TweetModel> tweets, Path distributionFilename) {
        System.out.println("");
        System.out.println("########## Distribution of Tweets Over Time ##########\n");
        List<String[]> distribution = createDistributionList(tweets);
        CSVHandler.write(distributionFilename, distribution);
        System.out.println("File saved to: "+distributionFilename);
        System.out.println("\n######################################################");
    }

    private static  Map<String, TermsTimeSeries> addSax(
            Map<String, TermsTimeSeries> ts, int alphabetSize, int timeWindow
    ) throws SAXException {
        double nThreshold = 0.0001;
        NormalAlphabet na = new NormalAlphabet();
        SAXProcessor sp = new SAXProcessor();
        ts = ts
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        x->x.getKey(),
                        x-> {
                            try {
                                double [] timeSeries = x.getValue().getOriginalTs();
                                int paaSize = timeSeries.length / timeWindow;
                                String sax = sp.ts2saxByChunking(
                                        timeSeries, paaSize, na.getCuts(alphabetSize), nThreshold
                                ).getSAXString("");
                                x.getValue().setSax(sax);
                                x.getValue().setW(paaSize);
                                return x.getValue();
                            } catch (SAXException e) {
                                e.printStackTrace();
                                return x.getValue();
                            }
                        }));
        return ts;
    }

    private static List<String[]> createClusterLists(
            Map<String, List<String>> clusters,
            Map<String, Long> freqs,
            Map<String, TermsTimeSeries> timeSeries) {
        List<String[]> rows= new ArrayList<String[]>();
        TermsTimeSeries tsFirst = (TermsTimeSeries) timeSeries.values().toArray()[0];
        String[] header = new String[5];
        header[0] = "term";
        header[1] = "cluster";
        header[2] = "total_tweets";
        header[3] = "date";
        header[4] = "number_of_tweets";
        rows.add(header);
        int cluster = 0;
        for(String clusterSax:clusters.keySet()) {
            for(String term : clusters.get(clusterSax)) {

                TermsTimeSeries termTs = timeSeries.get(term);
                if(termTs != null) {
                    double[] ts = timeSeries.get(term).getCompressedTs();
                    LocalDateTime[] dates = timeSeries.get(term).getCompressedTsDates();
                    for (int i = 0; i < ts.length; i++) {
                        String[] newEntry = new String[header.length];
                        newEntry[0] = term;
                        newEntry[1] = String.valueOf(cluster);
                        newEntry[2] = String.valueOf(freqs.get(term));
                        newEntry[3] = String.valueOf(dates[i]);
                        newEntry[4] = String.valueOf(ts[i]);
                        rows.add(newEntry);

                    }
                }
            }
            cluster += 1;
        }
        return rows;
    }

    private static Map<Integer, List<String>> getTopWordsByCluster(
            Map<String, List<String>> clusters,
            Map<String, Long> freqs,
            int topN
    ) {
        Map<Integer, List<String>> res = new HashMap<>();
        int cluster = 0;
        for(String clusterString : clusters.keySet()) {
            List<String> clusterTerms = clusters.get(clusterString);
            List<String> topFreqTerms = freqs.entrySet()
                    .stream()
                    .filter(x -> clusterTerms.contains(x.getKey()))
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .limit(topN)
                    .collect(Collectors.toMap(x->x.getKey(), x->x.getValue()))
                    .keySet().stream().collect(Collectors.toList());
            res.put(cluster, topFreqTerms);
            cluster += 1;
        }
        return res;
    }

    private static double[] compressTimeSeries(double [] oldTs, int window) {
        double [] newTs = new double[oldTs.length / window];
        int idxNewTs = 0;
        for(int i = 0; i<newTs.length; i++) {
            double sum = 0;
            for(int j = idxNewTs; j < idxNewTs + window; j++) {
                sum += oldTs[j];
            }
            newTs[i] = sum;
            idxNewTs += window;
        }
        return newTs;
    }

    private static LocalDateTime[] compressTermDates(LocalDateTime [] oldTs, int window) {
        LocalDateTime [] newTs = new LocalDateTime[oldTs.length / window];
        int idxNewTs = 0;
        int idxOldTs = 0;
        while(idxNewTs < newTs.length) {
            newTs[idxNewTs] = oldTs[idxOldTs];
            idxNewTs += 1;
            idxOldTs += 3;
        }
        return newTs;
    }

    private static Map<String, Set<Integer>> getTweetsTermInverseIndex(List<TweetModel> tweets) {
        Map<String, Set<Integer>> tweetsInverseIndex = new HashMap<>();
        int docId = 0;
        for(TweetModel tweet : tweets) {
            Set<String> termsVector = tweet.getTermsVector().keySet();
            for(String term: termsVector) {
                if (tweetsInverseIndex.containsKey(term)) {
                    tweetsInverseIndex.get(term).add(docId);
                } else {
                    Set<Integer> newSet = new HashSet<Integer>();
                    newSet.add(docId);
                    tweetsInverseIndex.put(term, newSet);
                }
            }
            docId += 1;
        }
        return tweetsInverseIndex;
    }

    private static List<String> getGraphComponents(String support, int clusterId) throws IOException {
        Path componentsPath = Paths.get(
                "graphs/"+support+"/k_core_graph_components"+clusterId+".txt");
        List<String> components = Files.readAllLines(componentsPath);
        return components;
    }

}
