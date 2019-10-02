package com.sbn.italianref.Handlers;

import it.stilo.g.algo.*;
import it.stilo.g.structures.Core;
import it.stilo.g.structures.DoubleValues;
import it.stilo.g.structures.WeightedDirectedGraph;
import it.stilo.g.structures.WeightedUndirectedGraph;
import it.stilo.g.util.NodesMapper;
import org.apache.commons.math3.util.Combinations;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;


public class GraphHandler {

    private static int runner = (int) (Runtime.getRuntime().availableProcessors());


    public static WeightedUndirectedGraph createCoGraph(
            List<String> clusterTerms, Map<String,
            Set<Integer>> tweetsTermsInverseIndex,
            NodesMapper<String> mapper,
            double threshold
    ) throws IOException {
        List<List<String>> combinations = getCombinations(clusterTerms);

        Map<List<String>, Double> graphNodes = new HashMap<>();
        for (List<String> combination : combinations) {
            graphNodes.put(combination, getCoocurrences(combination, tweetsTermsInverseIndex));
        }

        double min = graphNodes.entrySet().stream().min(Comparator.comparing(Map.Entry::getValue)).get().getValue();
        double max = graphNodes.entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).get().getValue();
/*        graphNodes = graphNodes.entrySet().stream()
                .collect(Collectors.toMap(x->x.getKey(), x-> normalize(x.getValue(), min, max)));*/

        graphNodes = graphNodes
                .entrySet()
                .stream()
                .filter(x -> x.getValue() > threshold)
                .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));

        WeightedUndirectedGraph g = new WeightedUndirectedGraph(graphNodes.size()+1);
        for(List<String> terms : graphNodes.keySet()) {
            g.testAndAdd(mapper.getId(terms.get(0)), mapper.getId(terms.get(1)), graphNodes.get(terms));
        }
        return g;
    }

    public static WeightedDirectedGraph readGzGraph(
            Path graphPath,
            Set<String> usersIds,
            NodesMapper<String> mapper
    ) throws IOException {
        GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(graphPath.toFile()));
        BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
        Scanner scanner = new Scanner(br);
        List<String[]> graphEdges = new ArrayList<>();
        Set<String> graphVertexes = new HashSet<>();
        while (scanner.hasNext()) {
            String[] graphEdge = scanner.nextLine().split("\t");
            if(usersIds.contains(graphEdge[0]) && usersIds.contains(graphEdge[1])) {
                graphEdges.add(graphEdge);
                graphVertexes.add(graphEdge[0]);
                graphVertexes.add(graphEdge[1]);
            }
        }

        WeightedDirectedGraph g = new WeightedDirectedGraph(graphVertexes.size()+1);
        for(String [] edge : graphEdges) {
            g.testAndAdd(mapper.getId(edge[1]), mapper.getId(edge[0]), Integer.parseInt(edge[2]));
        }

        return g;
    }

    private static List<List<String>>  getCombinations(List<String> terms) {
        List<List<String>> combinations = new ArrayList<>();
        Iterator<int[]> itr = new Combinations(terms.size(), 2).iterator();
        while (itr.hasNext()) {
            List<String> entry = new ArrayList<String>(2);
            int[] indexes = itr.next();
            if (indexes.length > 0) {
                for (int index : indexes) {
                    entry.add(terms.get(index));
                }
                combinations.add(entry);
            }
        }
        return combinations;
    }

    public static WeightedUndirectedGraph getLargestConnectedComponent(WeightedUndirectedGraph graph) throws InterruptedException {
        int[] all = new int[graph.size];
        for (int i = 0; i < graph.size; i++) {
            all[i] = i;
        }
        Set<Set<Integer>> ccs = ConnectedComponents.rootedConnectedComponents(graph, all, runner);
        int[] largestCC = ccs.stream().max(Comparator.comparing(Set::size)).get().stream().mapToInt(x -> x).toArray();
        WeightedUndirectedGraph copyGraph =  UnionDisjoint.copy(graph, runner);
        WeightedUndirectedGraph ccGraph = SubGraph.extract(copyGraph, largestCC, runner);
        return ccGraph;
    }

    public static WeightedDirectedGraph getLargestConnectedComponent(WeightedDirectedGraph graph) throws InterruptedException {
        int[] all = new int[graph.size];
        for (int i = 0; i < graph.size; i++) {
            all[i] = i;
        }
        Set<Set<Integer>> ccs = ConnectedComponents.rootedConnectedComponents(graph, all, runner);
        int[] largestCC = ccs.stream().max(Comparator.comparing(Set::size)).get().stream().mapToInt(x -> x).toArray();
        WeightedDirectedGraph copyGraph =  UnionDisjoint.copy(graph, runner);
        WeightedDirectedGraph ccGraph = SubGraph.extract(copyGraph, largestCC, runner);
        return ccGraph;
    }

    public static WeightedUndirectedGraph getKCore(WeightedUndirectedGraph graph) throws InterruptedException {
        WeightedUndirectedGraph graphCopy = UnionDisjoint.copy(graph, runner);
        Core core = CoreDecomposition.getInnerMostCore(graphCopy, runner);
        graphCopy = UnionDisjoint.copy(graph, runner);
        WeightedUndirectedGraph kCoreGraph = SubGraph.extract(graphCopy, core.seq, runner);
        return kCoreGraph;
    }

    public static Set<String> saveGraph(WeightedUndirectedGraph g, Path savePath, NodesMapper<String> mapper) throws IOException {
        Set<String> graphComponents = new HashSet<>();
        List<String> toWrite = new ArrayList<>();
        Set<String> processedNodes = new HashSet<>();
        for(int node = 0; node < g.in.length; node++) {
            if(g.in[node] != null) {
                for(int neighbor = 0; neighbor < g.in[node].length; neighbor++) {
                    String node1 = mapper.getNode(node);
                    String node2 = mapper.getNode(g.in[node][neighbor]);
                    graphComponents.add(node1);
                    if(processedNodes.contains(node2)) continue;
                    String weight = String.valueOf(g.weights[node][neighbor]);
                    String newString = node1 + " " + node2 + " "+weight;
                    processedNodes.add(node1);
                    toWrite.add(newString);
                }
            }
        }
        Files.write(savePath, toWrite, StandardCharsets.UTF_8);
        return graphComponents;
    }

    private static double getCoocurrences(List<String> terms, Map<String, Set<Integer>> tweetsTermsInverseIndex) {
        int res = 0;
        String term1 = terms.get(0);
        String term2 = terms.get(1);
        if(tweetsTermsInverseIndex.containsKey(term1) && tweetsTermsInverseIndex.containsKey(term2)){
            Set<Integer> set1 = tweetsTermsInverseIndex.get(term1);
            Set<Integer> set2 = tweetsTermsInverseIndex.get(term2);
            Set<Integer> intersection = new HashSet<Integer>();
            intersection.addAll(set1);
            intersection.retainAll(set2);
            res = intersection.size();
        }
        return res;
    }

        public static Map<Integer, Double> calculateHits(WeightedDirectedGraph g, int []nodes) {
            List<Integer> nodesList = Arrays.stream(nodes).boxed().collect(Collectors.toList());
            ArrayList<ArrayList<DoubleValues>> hits = HubnessAuthority.compute(g, 0.00001, runner);
            ArrayList<DoubleValues> hubScore= hits.get(1);
            Map<Integer, Double> hubs = hubScore
                    .stream()
                    .filter(x->nodesList.contains(x.index))
                    .collect(Collectors.toMap(
                            x->x.index,
                            x->x.value
                    ));
            return hubs;
        }
        public static Map<String, List<Double>> calculateHits(WeightedDirectedGraph g, NodesMapper<String> mapper) {
        Map<String, List<Double>> usersHits = new HashMap<>();
        ArrayList<ArrayList<DoubleValues>> hits = HubnessAuthority.compute(g, 0.00001, runner);
        ArrayList<DoubleValues> authorityScore = hits.get(0);
        ArrayList<DoubleValues> hubScore= hits.get(1);

        for(DoubleValues aut : authorityScore) {
            int node = aut.index;
            double score = aut.value;
            List<Double> newValues = new ArrayList<>() {{add(score);}};
            usersHits.put(mapper.getNode(node), newValues);
        }
        for(DoubleValues hub : hubScore) {
            int node = hub.index;
            double score = hub.value;
            usersHits.get(mapper.getNode(node)).add(score);
        }
        return usersHits;
    }

    public static  ArrayList<DoubleValues> kppNegSearchBroker(final WeightedDirectedGraph gAllVertex, final int[] nodes) throws InterruptedException {

        ArrayList<DoubleValues> listCg = new ArrayList<DoubleValues>();
        ArrayList<Integer> brokers = new ArrayList<Integer>();
        int cg = 0;
        int cgCompleteGraph = ReachabilityScore.compute(gAllVertex, runner);
        int totalNumOfNodes= nodes.length;
        int nodesProcessed = 0;
        String oldMessage = "Running KPP-NEG: 0.00% | Time Elapsed: 0m0s | Time Remaining: tbd";
        System.out.print(oldMessage);
        Instant start = Instant.now();
        for (int i = 0; i < nodes.length; i++) {
            WeightedDirectedGraph gv =  UnionDisjoint.copy(gAllVertex, runner);
            gv.remove(nodes[i]);
            cg = ReachabilityScore.compute(gv, runner);
            listCg.add(new DoubleValues(nodes[i], cgCompleteGraph-cg ));
            Instant end = Instant.now();
            long duration = Duration.between(start, end).getSeconds();
            nodesProcessed += 1;
            oldMessage = progressPct(oldMessage, nodesProcessed, totalNumOfNodes, duration);
        }
        System.out.println();
        Collections.sort(listCg);


        return listCg;
    }

    private static String progressPct(String oldMessage, int num, int total, long durationSeconds) {
        double pct = 100*((double) num) / total;
        int elapsedMinutes = (int) durationSeconds / 60;
        int elapsedSeconds = (int) durationSeconds - elapsedMinutes*60;

        int remainingSecondsTotal = (int) (durationSeconds * 100 / pct - durationSeconds);
        int remainingMinutes = (int) remainingSecondsTotal / 60;
        int remainingSeconds = (int) remainingSecondsTotal - remainingMinutes*60;

        int oldMessageSize = oldMessage.length();
        String backspaces = "\b".repeat(oldMessageSize*2);
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);
        String processingFiles =  "Running KPP-NEG: "+df.format(pct)+"%";
        String elapsed = "Time Elapsed: "+elapsedMinutes+"m"+elapsedSeconds+"s";
        String remaining = "Time Remaining: "+remainingMinutes+"m"+remainingSeconds+"s";
        String newMessage = processingFiles+" | "+elapsed+" | "+remaining;
        System.out.print(backspaces);
        System.out.print(newMessage);
        return newMessage;
    }

    public static WeightedDirectedGraph subsetGraph(WeightedDirectedGraph g, int degree) {
        List<Integer> nodes = new ArrayList<>();
        int [] vertexes = g.getVertex();
        for(int node : vertexes) {
            int inDegree = g.in[node] == null ? 0 : g.in[node].length;
            int outDegree = g.out[node] == null ? 0 : g.out[node].length;
            int nodeDegree = inDegree + outDegree;
            if(nodeDegree >= degree) {
                nodes.add(node);
            }
        }
        int [] nodesSubset = nodes.stream().mapToInt(x->x).toArray();
        WeightedDirectedGraph subsetGraph = SubGraph.extract(g, nodesSubset, runner);
        return subsetGraph;
    }

}
