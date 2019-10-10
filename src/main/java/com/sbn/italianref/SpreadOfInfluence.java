package com.sbn.italianref;


import com.sbn.italianref.Handlers.CSVHandler;
import com.sbn.italianref.Models.UserModel;
import it.stilo.g.structures.WeightedDirectedGraph;
import it.stilo.g.util.NodesMapper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class SpreadOfInfluence {


    public static void runLpa(
            WeightedDirectedGraph graph,
            NodesMapper<String> mapper,
            List<UserModel> users,
            int maxIterations,
            Path usersKPath,
            Path usersMPath,
            Path usersM2Path
    ) {
        System.out.println("Spread of Influence - LPA");
        System.out.println();

        System.out.print("[Spread of Influence - LPA] Using M starting...");
        lpa(graph, mapper, users, "M", maxIterations, usersMPath);
        System.out.println("DONE - File saved in "+usersMPath.toString());
        System.out.print("[Spread of Influence - LPA] Using M' starting...");
        lpa(graph, mapper, users, "M2", maxIterations, usersM2Path);
        System.out.println("DONE - File saved in "+usersM2Path.toString());
        System.out.print("[Spread of Influence - LPA] Using K starting...");
        lpa(graph, mapper, users, "K", maxIterations, usersKPath);
        System.out.println("DONE - File saved in "+usersKPath.toString());
        System.out.println();
        System.out.println("Spread of Influence - LPA DONE!");
        System.out.println();
    }

    public static void runKMeans(
            WeightedDirectedGraph graph,
            NodesMapper<String> mapper,
            List<UserModel> users,
            int maxIterations,
            Path usersKPath,
            Path usersMPath,
            Path usersM2Path
    ) {
        System.out.println("Spread of Influence - Modified LPA");
        System.out.println();

        System.out.print("[Spread of Influence - Modified LPA] Using M starting...");
        kMeans(graph, mapper, users, "M", maxIterations, usersMPath);
        System.out.println("DONE - File saved in "+usersMPath.toString());
        System.out.print("[Spread of Influence - Modified LPA] Using M' starting...");
        kMeans(graph, mapper, users, "M2", maxIterations, usersM2Path);
        System.out.println("DONE - File saved in "+usersM2Path.toString());
        System.out.print("[Spread of Influence - Modified LPA] Using K starting...");
        kMeans(graph, mapper, users, "K", maxIterations, usersKPath);
        System.out.println("DONE - File saved in "+usersKPath.toString());
        System.out.println();
        System.out.println("Spread of Influence - Modified LPA DONE!");
    }


    private static void lpa(
            WeightedDirectedGraph graph,
            NodesMapper<String> mapper,
            List<UserModel> users,
            String initialSeed,
            int maxIterations,
            Path filePath
    ) {
        int [] graphNodes = graph.getVertex();
        Set<Integer> graphNodesSet = Arrays.stream(graphNodes).boxed().collect(Collectors.toSet());
        Set<String> userIdSet = new HashSet<>();
        List<UserModel> usersFiltered = users
                .stream()
                .filter(x->graphNodesSet.contains(mapper.getId(x.getUserId())))
                .filter(x->userIdSet.add(x.getUserId()))
                .collect(Collectors.toList());

        Map<Integer, String> previousClusters = getInitialClusters(usersFiltered, mapper, initialSeed);
        List<String[]> fileRows = new ArrayList<>();
        String [] header = new String[] {"iteration", "support", "n"};
        fileRows.add(header);
        List<String[]> initialRows = getNewRows(previousClusters, -1);
        fileRows.addAll(initialRows);
        int countToBreak = 0;
        for(int i = 0; i<maxIterations; i++) {
            Map<Integer, String> newClusters = runLpaIteration(graph, previousClusters);
            List<String[]> newRows = getNewRows(newClusters, i);
            fileRows.addAll(newRows);
            if(newClusters.equals(previousClusters)){
                countToBreak += 1;
            } else {
                countToBreak = 0;
            }
            if(countToBreak == 10) break;
            previousClusters = newClusters;
        }
        CSVHandler.write(filePath, fileRows);
    }

    private static Map<Integer, String> runLpaIteration(WeightedDirectedGraph g, Map<Integer, String> clusters) {
        List<Integer> nodes = clusters.keySet().stream().collect(Collectors.toList());
        Collections.shuffle(nodes);
        Map<Integer, String> newClusters = nodes
                .stream()
                .collect(Collectors.toMap(
                        x->x,
                        x->getNeighborsHighestFreqLabel(g, clusters, x)
                ));
        return newClusters;
    }

    private static String getNeighborsHighestFreqLabel(WeightedDirectedGraph g, Map<Integer, String> clusters, int node) {
        int neighborsFreq = 0;
        String label = getRandomLabel();
        int [] neighbors = g.out[node];
        if(neighbors != null) {
            for(int n : neighbors) {
                if(clusters.get(n).equals("Yes")) neighborsFreq += 1;
                else if (clusters.get(n).equals("No")) neighborsFreq -= 1;
            }
        }
        if(neighborsFreq > 0) label = "Yes";
        else if (neighborsFreq < 0) label = "No";
        return label;
    }

    private static List<String[]> getNewRows(Map<Integer, String> clusters, int iteration) {
        List<String[]> newRows = new ArrayList<>();
        Map<String, Long> supportersState = clusters
                .values()
                .stream()
                .collect(Collectors.groupingBy(x->x, Collectors.counting()));
        for(String support : supportersState.keySet()) {
            String [] row = new String[3];
            row[0] = String.valueOf(iteration);
            row[1] = support;
            row[2] = String.valueOf(supportersState.get(support));
            newRows.add(row);
        }
/*
        System.out.println("Iteration "+iteration+": "+supportersState);
*/
        return newRows;
    }

    private static String getRandomLabel() {
        Random randomizer = new Random(System.currentTimeMillis());
        String [] labels = new String[] {"Yes", "No"};
        return labels[randomizer.nextInt(labels.length)];
    }

    private static Map<Integer, String> getInitialClusters(
            List<UserModel> usersModel,
            NodesMapper<String> mapper,
            String initialSeed
    ) {
        Map<Integer, String> initialClusters = new HashMap<>();
        List<String> usersYes = null;
        List<String> usersNo = null;
        switch (initialSeed) {
            case "M":
                usersYes = usersModel
                        .stream()
                        .filter(x->x.getSupport().equals("Yes"))
                        .map(x->x.getUserId())
                        .collect(Collectors.toList());
                usersNo = usersModel
                        .stream()
                        .filter(x->x.getSupport().equals("No"))
                        .map(x->x.getUserId())
                        .collect(Collectors.toList());
                break;
            case "M2":
                usersYes = usersModel
                        .stream()
                        .filter(x->x.getSupport().equals("Yes"))
                        .sorted(Comparator.comparing(UserModel::getCentralityScore).reversed())
                        .limit(1000)
                        .map(x->x.getUserId())
                        .collect(Collectors.toList());
                usersNo = usersModel
                        .stream()
                        .filter(x->x.getSupport().equals("No"))
                        .sorted(Comparator.comparing(UserModel::getCentralityScore).reversed())
                        .limit(1000)
                        .map(x->x.getUserId())
                        .collect(Collectors.toList());
                break;
            case "K":
                usersYes = getKUsers("yes", usersModel);
                usersNo = getKUsers("no", usersModel);
                break;
            default: break;
        }
        for(UserModel userModel : usersModel) {
            if(usersYes.contains(userModel.getUserId()))
                initialClusters.put(mapper.getId(userModel.getUserId()), "Yes");
            else if(usersNo.contains(userModel.getUserId()))
                initialClusters.put(mapper.getId(userModel.getUserId()), "No");
            else
                initialClusters.put(mapper.getId(userModel.getUserId()), "Neutral");
        }
        return initialClusters;
    }

    private static void kMeans(
            WeightedDirectedGraph graph,
            NodesMapper<String> mapper,
            List<UserModel> users,
            String initialSeed,
            int maxIterations,
            Path filePath
    ){
        int [] graphNodes = graph.getVertex();
        Set<Integer> graphNodesSet = Arrays.stream(graphNodes).boxed().collect(Collectors.toSet());
        Set<String> userIdSet = new HashSet<>();
        List<UserModel> usersFiltered = users
                .stream()
                .filter(x->graphNodesSet.contains(mapper.getId(x.getUserId())))
                .filter(x->userIdSet.add(x.getUserId()))
                .collect(Collectors.toList());

        Map<Integer, String> previousClusters = getInitialClusters(usersFiltered, mapper, initialSeed);
        Map<String, Integer> previousCentroids = getInitialCentroids(previousClusters);
        List<String[]> fileRows = new ArrayList<>();
        String [] header = new String[] {"iteration", "support", "n"};
        fileRows.add(header);
        List<String[]> initialRows = getNewRows(previousClusters, -1);
        fileRows.addAll(initialRows);
        int countToBreak = 0;
        for(int i = 0; i < maxIterations; i++) {
            Map<Integer, String> newClusters = runKMeansIteration(graph, previousClusters, previousCentroids);
            Map<String, Integer> newCentroids = getNewCentroids(graph, previousClusters);
            List<String[]> newRows = getNewRows(newClusters, i);
            fileRows.addAll(newRows);
            if(newClusters.equals(previousClusters)){
                countToBreak += 1;
            } else {
                countToBreak = 0;
            }
            if(countToBreak == 10) break;
            previousClusters = newClusters;
            previousCentroids = newCentroids;
        }
        CSVHandler.write(filePath, fileRows);
    }

    private static Map<Integer, String> runKMeansIteration(WeightedDirectedGraph g, Map<Integer, String> clusters, Map<String, Integer> centroids) {
        Map<Integer, String> newClusters = new HashMap<>();
        for(int node : clusters.keySet()) {
            Map<String, Double> results = calculateScore(g, clusters, node, centroids.get(clusters.get(node)));
            double score = results.get("Yes") - results.get("No");

            String newCluster = "";
            if(score == 0) newCluster = getRandomLabel();
            else newCluster = score > 0 ? "Yes" : "No";
            newClusters.put(node, newCluster);
        }
        return newClusters;
    }

    private static Map<String, Integer> getNewCentroids(WeightedDirectedGraph g, Map<Integer, String> clusters) {
        List<Integer> yesNodes = clusters
                .entrySet()
                .stream()
                .filter(x->x.getValue().equals("Yes"))
                .map(x->x.getKey())
                .collect(Collectors.toList());
        List<Integer> noNodes = clusters
                .entrySet()
                .stream()
                .filter(x->x.getValue().equals("No"))
                .map(x->x.getKey())
                .collect(Collectors.toList());

        Map<Integer, Map<String, Double>> yesNodesScores = yesNodes
                .stream()
                .collect(Collectors.toMap(
                        x->x,
                        x->calculateCentroidScore(g, clusters, x)
                ));
        Map<Integer, Map<String, Double>> noNodesScores = noNodes
                .stream()
                .collect(Collectors.toMap(
                        x->x,
                        x->calculateCentroidScore(g, clusters, x)
                ));




        int yesCentroid = 0;
        double maxYes = -1;
        int noCentroid = 0;
        double maxNo = -1;
        for(int node : yesNodesScores.keySet()) {
            Map<String, Double> score = yesNodesScores.get(node);
            double yesScore = score.get("Yes") - score.get("No");
            if(yesScore > maxYes) {
                maxYes = yesScore;
                yesCentroid = node;
            }
        }
        for(int node : noNodesScores.keySet()) {
            Map<String, Double> score = noNodesScores.get(node);
            double noScore = score.get("No") - score.get("Yes");
            if(noScore > maxNo) {
                maxNo = noScore;
                noCentroid = node;
            }
        }

        Map<String, Integer> centroids = new HashMap<>();

        centroids.put("Yes", yesCentroid);
        centroids.put("No", noCentroid);
        return centroids;
    }

    private static Map<String, Double> calculateScore(
            WeightedDirectedGraph g,
            Map<Integer, String> clusters,
            int node,
            int centroid
    ) {
        Map<String, Double> results = new HashMap<>();
        results.put("Yes", 0.);
        results.put("No", 0.);
        int[] neighbors = g.out[node];
        int[] centroidNeighbors = centroid != -1 ? g.out[centroid] : null;
        Map<String, Integer> neighborsResults = calculaceScoreAux(clusters, neighbors);
        Map<String, Integer> centroidNeighborsResults = calculaceScoreAux(clusters, centroidNeighbors);
        results.put("Yes", results.get("Yes") + 1.5*neighborsResults.get("Yes") + 0.5*centroidNeighborsResults.get("Yes"));
        results.put("No", results.get("No") + 1.5*neighborsResults.get("No") + 0.5*centroidNeighborsResults.get("No"));
        if(neighbors != null) {
            for(int n : neighbors) {
                Map<String, Integer> nResults = calculaceScoreAux(clusters, g.out[n]);
                results.put("Yes", results.get("Yes") + 0.8*nResults.get("Yes"));
                results.put("No", results.get("No") + 0.8*nResults.get("No"));
            }
        }
        return results;
    }

    private static Map<String, Double> calculateCentroidScore(
            WeightedDirectedGraph g,
            Map<Integer, String> clusters,
            int node
    ) {
        Map<String, Double> results = new HashMap<>();
        results.put("Yes", 0.);
        results.put("No", 0.);
        int[] neighbors = g.out[node];
        Map<String, Integer> neighborsResults = calculaceScoreAux(clusters, neighbors);
        results.put("Yes", results.get("Yes") + 1.5*neighborsResults.get("Yes"));
        results.put("No", results.get("No") + 1.5*neighborsResults.get("No"));
        if(neighbors != null) {
            for(int n : neighbors) {
                Map<String, Integer> nResults = calculaceScoreAux(clusters, g.out[n]);
                results.put("Yes", results.get("Yes") + nResults.get("Yes"));
                results.put("No", results.get("No") + nResults.get("No"));
            }
        }
        return results;
    }

    private static Map<String, Integer> calculaceScoreAux(Map<Integer, String> clusters, int [] nodes) {
        int yesScore = 0;
        int noScore = 0;
        if(nodes != null) {
            for(int i : nodes) {
                if(clusters.get(i).equals("Yes")) {
                    yesScore += 1;
                } else if (clusters.get(i).equals("No")) {
                    noScore += 1;
                }
            }
        }
        Map<String, Integer> results = new HashMap<>();
        results.put("Yes", yesScore);
        results.put("No", noScore);
        return results;
    }

    private static Map<String, Integer> getInitialCentroids(Map<Integer, String> clusters) {
        Map<String, Integer> centroids = new HashMap<>();
        List<Integer> initialYes = clusters
                .entrySet()
                .stream()
                .filter(x->x.getValue().equals("Yes"))
                .map(x->x.getKey())
                .collect(Collectors.toList());
        List<Integer> initialNo = clusters
                .entrySet()
                .stream()
                .filter(x->x.getValue().equals("No"))
                .map(x->x.getKey())
                .collect(Collectors.toList());

        Random randomizer = new Random();
        int noCentroid = initialNo.get(randomizer.nextInt(initialNo.size()));
        int yesCentroid = initialYes.get(randomizer.nextInt(initialYes.size()));
        centroids.put("Yes", yesCentroid);
        centroids.put("No", noCentroid);
        centroids.put("Neutral", -1);

        return centroids;

    }

    private static List<String> getKUsers(String support, List<UserModel> usersModel) {
        Path filePath = Paths.get("identifying_yes_no_supporters/kpp_score_"+support+".csv");
        List<String[]> users = CSVHandler.read(filePath, true);
        Map<String, Double> usersScore = new HashMap<>();
        for(String[] row : users) {
            usersScore.put(row[0], Double.valueOf(row[1]));
        }
        usersScore = usersScore
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(500)
                .collect(Collectors.toMap(x->x.getKey(), x->x.getValue()));
        Map<String, Double> finalUsersScore = usersScore;
        List<String> usersId = usersModel
                .stream()
                .filter(x-> finalUsersScore.containsKey(x.getUserName()))
                .map(x->x.getUserId())
                .collect(Collectors.toList());
        return usersId;
    }

}
