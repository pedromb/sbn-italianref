package com.sbn.italianref;

import com.sbn.italianref.Handlers.CSVHandler;
import com.sbn.italianref.Handlers.GraphHandler;
import com.sbn.italianref.Handlers.TweetsHandler;
import com.sbn.italianref.Models.TermsTimeSeriesModel;
import com.sbn.italianref.Models.TweetModel;
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
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class TemporalAnalysis {

    public static List<TweetModel> getUsersSupport(
            TweetsHandler tweetsHandler, HashMap<String, String> users, Path pathToSave
    ) throws IOException {
        System.out.println("");
        System.out.println("Temporal Analysis");
        System.out.println("");
        System.out.println("[Temporal Analysis - Part 1] Part 1 starting!");
        String luceneQuery = constructLuceneQueryForUser(users.keySet());
        List<String> fields = new ArrayList<>(){{add("user");}};
        System.out.print("[Temporal Analysis - Part 1] Retrieving tweets from lucene index...");
        List<TweetModel> tweets = tweetsHandler.queryTweets(fields, luceneQuery);
        tweets = tweetsHandler.addSupportToTweetModel(tweets, users);
        System.out.println("DONE");
        System.out.print("[Temporal Analysis - Part 1] Saving tweets distribution over time...");
        saveTweetsByUserBySupportByTime(tweets, pathToSave);
        System.out.println("DONE - File saved in "+pathToSave.toString());
        System.out.println("[Temporal Analysis - Part 1] Part 1 DONE!");
        return tweets;
    }

    public static Map<String, Map<String, List<String>>> saxClusters(
            TweetsHandler tweetsHandler,
            List<TweetModel> tweets,
            int timeWindow,
            int alphabetSize,
            int k,
            int maxIterations,
            Path clustersPath
    ) throws IOException, SAXException {
        System.out.println("");
        System.out.println("[Temporal Analysis - Part 2] Part 2 starting!");
        List<TweetModel> tweetsYes = tweets.stream()
                .filter(x -> x.getActualSupport().equals("yes"))
                .collect(Collectors.toList());
        List<TweetModel> tweetsNo = tweets.stream()
                .filter(x -> x.getActualSupport().equals("no"))
                .collect(Collectors.toList());
        Set<String> usersYes = tweetsYes
                .stream()
                .map(TweetModel::getUser)
                .collect(Collectors.toSet());
        Set<String> usersNo = tweetsNo
                .stream()
                .map(TweetModel::getUser)
                .collect(Collectors.toSet());

        String luceneQueryYes = constructLuceneQueryForUser(usersYes);
        String luceneQueryNo = constructLuceneQueryForUser(usersNo);

        NormalAlphabet na = new NormalAlphabet();
        double[][] distanceMatrix = na.getDistanceMatrix(alphabetSize);

        System.out.print("[Temporal Analysis - Part 2] Getting top 1000 words by frequency for YES supporters...");
        Map<String, Long> termsFreqYes = tweetsHandler.getTermsFrequency("user", luceneQueryYes, 1000);
        System.out.println("DONE.");
        System.out.print("[Temporal Analysis - Part 2] Creating sax strings for each word (12h grain)...");
        Map<String, TermsTimeSeriesModel> timeseriesYes = getTermsTimeseries(termsFreqYes, tweetsYes, timeWindow);
        timeseriesYes = addSax(timeseriesYes, alphabetSize, timeWindow);
        System.out.println("DONE.");

        System.out.print("[Temporal Analysis - Part 2] Getting top 1000 words by frequency for NO supporters...");
        Map<String, Long> termsFreqNo = tweetsHandler.getTermsFrequency("user", luceneQueryNo, 1000);
        System.out.println("DONE.");
        System.out.print("[Temporal Analysis - Part 2] Creating sax strings for each word (12h grain)...");
        Map<String, TermsTimeSeriesModel> timeseriesNo = getTermsTimeseries(termsFreqNo, tweetsNo, timeWindow);
        timeseriesNo = addSax(timeseriesNo, alphabetSize, timeWindow);
        System.out.println("DONE.");


        System.out.print("[Temporal Analysis - Part 2] Running KMeans for YES words...");
        Map<String, List<String>> clustersYes = KMeans.fitForSax(timeseriesYes, k, distanceMatrix, alphabetSize, maxIterations);
        System.out.println("DONE.");


        System.out.print("[Temporal Analysis - Part 2] Running KMeans for NO words...");
        Map<String, List<String>> clustersNo = KMeans.fitForSax(timeseriesNo, k, distanceMatrix, alphabetSize, maxIterations);
        System.out.println("DONE.");

        System.out.print("[Temporal Analysis - Part 2] Saving clusters file...");
        List<String[]> rows = createClustersCsv(clustersYes, clustersNo, termsFreqYes, termsFreqNo);
        CSVHandler.write(clustersPath, rows);
        System.out.println("DONE - File saved in "+clustersPath.toString());
        System.out.println("[Temporal Analysis - Part 2] Part 2 DONE!");
        System.out.println();

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

        System.out.println("[Temporal Analysis - Part 3] Part 3 starting!");

        Map<String, List<String>> yesClusters = clusterBySupport.get("yes");
        Map<String, List<String>> noClusters = clusterBySupport.get("no");

        List<TweetModel> tweetsYes = tweets.stream()
                .filter(x -> x.getActualSupport().equals("yes"))
                .collect(Collectors.toList());

        List<TweetModel> tweetsNo = tweets.stream()
                .filter(x -> x.getActualSupport().equals("no"))
                .collect(Collectors.toList());


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

        System.out.println("[Temporal Analysis - Part 3] Processing graphs for YES tokens...");
        Map<String, Set<Integer>> yesTweetsTermsInverseIndex = getTweetsTermInverseIndex(tweetsYes);
        int clusterId = 1;
        for(List<String> clusterTerms: yesClusters.values()) {
            processGraphs(clusterTerms, yesTweetsTermsInverseIndex, 0.1, yesGraphsPath, clusterId);
            clusterId += 1;
        }
        System.out.println("[Temporal Analysis - Part 3] Finished processing graphs for YES tokens!");

        System.out.println("[Temporal Analysis - Part 3] Processing graphs for NO tokens...");

        Map<String, Set<Integer>> noTweetsTermsInverseIndex = getTweetsTermInverseIndex(tweetsNo);
        clusterId = 1;
        for(List<String> clusterTerms: noClusters.values()) {
            processGraphs(clusterTerms, noTweetsTermsInverseIndex, 0.1, noGraphsPath, clusterId);
            clusterId += 1;
        }
        System.out.println("[Temporal Analysis - Part 3] Finished processing graphs for NO tokens!");
        System.out.println("[Temporal Analysis - Part 3] Part 3 DONE!");
        System.out.println();



    }

    public static void coreComponentsTimeseries(
            TweetsHandler tw,
            List<TweetModel> tweets,
            int k,
            Path kCoreTimeseriesYesPath,
            Path kCoreTimeseriesNoPath
    ) throws IOException {
        System.out.println("[Temporal Analysis - Part 4] Part 4 starting!");

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

        System.out.print("[Temporal Analysis - Part 4] Getting timeseries for each k-core obtatined in part 3...");

        for(int i = 1; i<k+1; i++) {
            int clusterId = i;
            List<String> yesComponents = getGraphComponents("yes", clusterId);
            Map<String, TermsTimeSeriesModel> timeseriesYes = getTermsTimeseries(yesTermsFreq, tweetsYes, 3);
            timeseriesYes = timeseriesYes.entrySet()
                    .stream()
                    .filter(x -> yesComponents.contains(x.getValue().getTerm()))
                    .collect(Collectors.toMap(x -> x.getKey(), x-> x.getValue()));
            List<String[]> yesNewRows = getGraphRows(timeseriesYes, clusterId);
            yesRows.addAll(yesNewRows);

            List<String> noComponents = getGraphComponents("no", clusterId);
            Map<String, TermsTimeSeriesModel> timeseriesNo = getTermsTimeseries(noTermsFreq, tweetsNo, 3);
            timeseriesNo = timeseriesNo.entrySet()
                    .stream()
                    .filter(x -> noComponents.contains(x.getValue().getTerm()))
                    .collect(Collectors.toMap(x -> x.getKey(), x-> x.getValue()));
            List<String[]> noNewsRo = getGraphRows(timeseriesNo, clusterId);
            noRows.addAll(noNewsRo);
        }
        System.out.println("DONE.");
        System.out.print("[Temporal Analysis - Part 4] Saving files for YES groups...");
        CSVHandler.write(kCoreTimeseriesYesPath, yesRows);
        System.out.println("DONE - File saved in "+kCoreTimeseriesYesPath.toString());
        System.out.print("[Temporal Analysis - Part 4] Saving files for NO groups...");
        CSVHandler.write(kCoreTimeseriesNoPath, noRows);
        System.out.println("DONE - File saved in "+kCoreTimeseriesNoPath.toString());
        System.out.println("[Temporal Analysis - Part 4] Part 4 DONE!");
        System.out.println();
        System.out.println("Temporal Analysis DONE!");
        System.out.println();

    }

    private static List<String[]> getGraphRows(Map<String, TermsTimeSeriesModel> timeseries, int clusterId) {
        List<String[]> rows= new ArrayList<String[]>();
        String cluster = String.valueOf(clusterId);
        for(String term : timeseries.keySet()) {
            TermsTimeSeriesModel termTs = timeseries.get(term);
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
        System.out.print("[Temporal Analysis - Part 3] Cluster "+clusterId+": Creating co-occurrence graph...");
        WeightedUndirectedGraph yesGraph = GraphHandler.createCoGraph(clusterTerms, tweetsTermsInverseIndex, mapper, threshold);
        System.out.println("DONE.");
        System.out.print("[Temporal Analysis - Part 3] Cluster "+clusterId+": Getting largest connected component...");
        WeightedUndirectedGraph yesCCGraph = GraphHandler.getLargestConnectedComponent(yesGraph);
        System.out.println("DONE.");
        System.out.print("[Temporal Analysis - Part 3] Cluster "+clusterId+": Getting k-core...");
        WeightedUndirectedGraph yesKCoreGraph = GraphHandler.getKCore(yesGraph);
        System.out.println("DONE.");

        System.out.print("[Temporal Analysis - Part 3] Cluster "+clusterId+": Saving files...");

        Path fullGraphPath = Paths.get(fullGraphFolder);
        Path largestCCPath = Paths.get(largestCCFolder);
        Path kCorePath = Paths.get(kCoreFolder);

        Set<String> fullGraphComponents = GraphHandler.saveGraph(yesGraph, fullGraphPath, mapper);
        Set<String> coreComponentComponents = GraphHandler.saveGraph(yesCCGraph, largestCCPath, mapper);
        Set<String> kCoreComponents = GraphHandler.saveGraph(yesKCoreGraph, kCorePath, mapper);

        String fullGraphComponentsFolder = graphsPath.toString() + "/full_graph_components" + clusterId + ".txt";
        String largestCCComponentsFolder = graphsPath.toString() + "/largest_cc_graph_components" + clusterId + ".txt";
        String kCoreComponentsFolder = graphsPath.toString() + "/k_core_graph_components" + clusterId + ".txt";
        Path fullGraphComponentsPath = Paths.get(fullGraphComponentsFolder);
        Path largestCCComponentsPath = Paths.get(largestCCComponentsFolder);
        Path kCoreComponentsPath = Paths.get(kCoreComponentsFolder);

        Files.write(fullGraphComponentsPath, fullGraphComponents, StandardCharsets.UTF_8);
        Files.write(largestCCComponentsPath, coreComponentComponents, StandardCharsets.UTF_8);
        Files.write(kCoreComponentsPath, kCoreComponents, StandardCharsets.UTF_8);
        String filesLocation = graphsPath.toString() + "/*" + clusterId + ".txt";
        System.out.println("DONE - Files saved in folder "+filesLocation);


    }

    private static Map<String, TermsTimeSeriesModel> getTermsTimeseries(Map<String, Long> terms, List<TweetModel> tweets, int timeWindow) {
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
        Map<String, TermsTimeSeriesModel> termsTimeSeries = new HashMap<String, TermsTimeSeriesModel>();
        for(TweetModel tweet : tweets) {
            Map<String, Long> termsVector = tweet.getTermsVector();
            for(String term : termsVector.keySet()) {
                if(terms.containsKey(term)) {
                    LocalDateTime termDate = tweet.getCreatedAtTruncatedHours();
                    int position = intervals.indexOf(termDate);
                    long termFreq = termsVector.get(term);
                    if(termsTimeSeries.containsKey(term)) {
                        TermsTimeSeriesModel ts = termsTimeSeries.get(term);
                        ts.addToOriginalTs(termFreq, position);
                    } else {
                        double[] ts = new double[intervals.size()];
                        Arrays.fill(ts, 0);
                        ts[position] = termFreq;
                        TermsTimeSeriesModel newTs = new TermsTimeSeriesModel();
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

    private static void saveTweetsByUserBySupportByTime(List<TweetModel> tweets, Path pathToSave) {
        List<String[]> rows = new ArrayList<>();
        String[] header = new String[] {"support", "user", "created_at"};
        rows.add(header);
        for (TweetModel tweet: tweets) {
            String[] row = new String[header.length];
            row[0] = tweet.getActualSupport();
            row[1] = tweet.getUser();
            row[2] = tweet.getCreatedAtTruncatedHours().toString();
            rows.add(row);
        }
        CSVHandler.write(pathToSave, rows);
    }

    private static  Map<String, TermsTimeSeriesModel> addSax(
            Map<String, TermsTimeSeriesModel> ts, int alphabetSize, int timeWindow
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

    private static List<String[]> createClustersCsv(
            Map<String, List<String>> clustersYes,
            Map<String, List<String>> clustersNo,
            Map<String, Long> freqsYes,
            Map<String, Long> freqsNo
    ) {
        List<String[]> rows= new ArrayList<String[]>();
        String[] header = new String[4];
        header[0] = "term";
        header[1] = "cluster";
        header[2] = "support";
        header[3] = "number_of_tweets";
        rows.add(header);
        rows = addRowsToListClusters(rows, clustersYes, freqsYes, "yes");
        rows = addRowsToListClusters(rows, clustersNo, freqsNo, "no");

        return rows;
    }

    private static List<String[]> addRowsToListClusters(
            List<String[]> rows,
            Map<String, List<String>> clusters,
            Map<String, Long> freqs,
            String support
    ) {

        int stringLength = rows.get(0).length;
        int clusterId = 1;
        for(String cluster : clusters.keySet()) {
            List<String> clusterTerms = clusters.get(cluster);
            for (String term : clusterTerms) {
                String [] newRow = new String[stringLength];
                newRow[0] = term;
                newRow[1] = String.valueOf(clusterId);
                newRow[2] = support;
                newRow[3] = String.valueOf(freqs.get(term));
                rows.add(newRow);
            }
            clusterId += 1;
        }
        return rows;

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
                "temporal_analysis/graphs/"+support+"/k_core_graph_components"+clusterId+".txt");
        List<String> components = Files.readAllLines(componentsPath);
        return components;
    }

    private static String constructLuceneQueryForUser(Set<String> users) {
        String luceneQuery = "";
        for(String user : users) {
            luceneQuery += luceneQuery.length() == 0 ? user : " || "+user;

        }
        return luceneQuery;
    }

}
