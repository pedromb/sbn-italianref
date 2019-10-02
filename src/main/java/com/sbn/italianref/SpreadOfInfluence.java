package com.sbn.italianref;


import com.sbn.italianref.Handlers.CSVHandler;
import com.sbn.italianref.Models.UserModel;
import it.stilo.g.structures.WeightedDirectedGraph;
import it.stilo.g.util.NodesMapper;
import org.jetbrains.annotations.NotNull;
import twitter4j.User;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class SpreadOfInfluence {


    public static void run(
            WeightedDirectedGraph graph,
            NodesMapper<String> mapper,
            List<UserModel> users,
            int maxIterations,
            Path usersKPath,
            Path usersMPath,
            Path usersM2Path
    ) {
        System.out.println("Spread of Influence");
        System.out.println();

        System.out.print("[Spread of Influence] Using M starting...");
        lpa(graph, mapper, users, "M", maxIterations, usersMPath);
        System.out.println("DONE - File saved in "+usersMPath.toString());
        System.out.print("[Spread of Influence] Using M' starting...");
        lpa(graph, mapper, users, "M2", maxIterations, usersM2Path);
        System.out.println("DONE - File saved in "+usersM2Path.toString());
        System.out.println();
        System.out.println("Spread of Influence DONE!");
    }

    public static void lpa(
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
        for(int i = 0; i<maxIterations; i++) {
            Map<Integer, String> newClusters = runLpaIteration(graph, previousClusters);
            List<String[]> newRows = getNewRows(newClusters, i);
            fileRows.addAll(newRows);
            if(newClusters.equals(previousClusters)) break;
            previousClusters = newClusters;
        }
        CSVHandler.write(filePath, fileRows);
    }

    public static Map<Integer, String> runLpaIteration(WeightedDirectedGraph g, @NotNull Map<Integer, String> clusters) {
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

    public static String getNeighborsHighestFreqLabel(WeightedDirectedGraph g, Map<Integer, String> clusters, int node) {
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

    public static List<String[]> getNewRows(Map<Integer, String> clusters, int iteration) {
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

    public static String getRandomLabel() {
        Random randomizer = new Random(System.currentTimeMillis());
        String [] labels = new String[] {"Yes", "No"};
        return labels[randomizer.nextInt(labels.length)];
    }

    public static Map<Integer, String> getInitialClusters(
            List<UserModel> usersModel,
            NodesMapper<String> mapper,
            String initialSeed
    ) {
        Map<Integer, String> initialClusters = new HashMap<>();
        switch (initialSeed) {
            case "M":
                initialClusters = usersModel
                        .stream()
                        .collect(Collectors.toMap(
                                x->mapper.getId(x.getUserId()),
                                x->x.getSupport()
                        ));
                break;
            case "M2":
                List<String> m2UsersYes = usersModel
                        .stream()
                        .filter(x->x.getSupport().equals("Yes"))
                        .sorted(Comparator.comparing(UserModel::getCentralityScore).reversed())
                        .limit(1000)
                        .map(x->x.getUserId())
                        .collect(Collectors.toList());
                List<String> m2UsersNo = usersModel
                        .stream()
                        .filter(x->x.getSupport().equals("No"))
                        .sorted(Comparator.comparing(UserModel::getCentralityScore).reversed())
                        .limit(1000)
                        .map(x->x.getUserId())
                        .collect(Collectors.toList());
                for(UserModel userModel : usersModel) {
                    if(m2UsersYes.contains(userModel.getUserId()))
                        initialClusters.put(mapper.getId(userModel.getUserId()), "Yes");
                    else if(m2UsersNo.contains(userModel.getUserId()))
                        initialClusters.put(mapper.getId(userModel.getUserId()), "No");
                    else
                        initialClusters.put(mapper.getId(userModel.getUserId()), "Neutral");
                }

            default: break;
        }
        return initialClusters;
    }

}
