package com.sbn.italianref;

import it.stilo.g.algo.*;
import it.stilo.g.structures.Core;
import it.stilo.g.structures.DoubleValues;
import it.stilo.g.structures.WeightedUndirectedGraph;
import it.stilo.g.util.NodesMapper;
import org.apache.commons.math3.util.Combinations;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;


public class GraphWrapper {

    private static int runner = (int) (Runtime.getRuntime().availableProcessors());
    private static NodesMapper<String> internalMapper;


    public static WeightedUndirectedGraph createCoGraph(
            List<String> clusterTerms, Map<String,
            Set<Integer>> tweetsTermsInverseIndex,
            NodesMapper<String> mapper,
            double threshold
    ) throws IOException {
        internalMapper = mapper;
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
            g.testAndAdd(internalMapper.getId(terms.get(0)), internalMapper.getId(terms.get(1)), graphNodes.get(terms));
        }
        return g;
    }

    public static WeightedUndirectedGraph readGzGraph(Path graphPath, Set<String> usersIds, NodesMapper<String> mapper) throws IOException {
        internalMapper = mapper;
        GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(graphPath.toFile()));
        BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
        Scanner scanner = new Scanner(br);
        List<String[]> graphEdges = new ArrayList<>();
        Set<String> graphVertexes = new HashSet<>();
        System.out.println("Reading graph file (this could take awhile)...");
        while (scanner.hasNext()) {
            String[] graphEdge = scanner.nextLine().split("\t");
            if(usersIds.contains(graphEdge[0]) && usersIds.contains(graphEdge[1])) {
                graphEdges.add(graphEdge);
                graphVertexes.add(graphEdge[0]);
                graphVertexes.add(graphEdge[1]);
            }
        }

        WeightedUndirectedGraph g = new WeightedUndirectedGraph(graphVertexes.size()+1);
        for(String [] edge : graphEdges) {
            g.testAndAdd(internalMapper.getId(edge[0]), internalMapper.getId(edge[1]), Integer.parseInt(edge[2]));
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
        WeightedUndirectedGraph graphCopy = UnionDisjoint.copy(graph, runner);
        Set<Set<Integer>> ccs = ConnectedComponents.rootedConnectedComponents(graphCopy, all, runner);
        int[] largestCC = ccs.stream().max(Comparator.comparing(Set::size)).get().stream().mapToInt(x -> x).toArray();
        graphCopy = UnionDisjoint.copy(graph, runner);
        WeightedUndirectedGraph ccGraph = SubGraph.extract(graphCopy, largestCC, runner);
        return ccGraph;
    }

    public static WeightedUndirectedGraph getKCore(WeightedUndirectedGraph graph) throws InterruptedException {
        WeightedUndirectedGraph graphCopy = UnionDisjoint.copy(graph, runner);
        Core core = CoreDecomposition.getInnerMostCore(graphCopy, runner);
        graphCopy = UnionDisjoint.copy(graph, runner);
        WeightedUndirectedGraph kCoreGraph = SubGraph.extract(graphCopy, core.seq, runner);
        return kCoreGraph;
    }

    public static Set<String> saveGraph(WeightedUndirectedGraph g, Path savePath) throws IOException {
        Set<String> graphComponents = new HashSet<>();
        List<String> toWrite = new ArrayList<>();
        Set<String> processedNodes = new HashSet<>();
        for(int node = 0; node < g.in.length; node++) {
            if(g.in[node] != null) {
                for(int neighbor = 0; neighbor < g.in[node].length; neighbor++) {
                    String node1 = internalMapper.getNode(node);
                    String node2 = internalMapper.getNode(g.in[node][neighbor]);
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

    public static Map<String, Double> calculateHubnessAuthority(WeightedUndirectedGraph g) {
        Map<String, Double> usersHubnessAuthority = new HashMap<>();
        ArrayList<ArrayList<DoubleValues>> hits = HubnessAuthority.compute(g, 0.00001, runner);
        ArrayList<DoubleValues> hubAuthority = hits.get(1);
        for(DoubleValues hub : hubAuthority) {
            int node = hub.index;
            double hubScore = hub.value;
            usersHubnessAuthority.put(internalMapper.getNode(node), hubScore);
        }
        return usersHubnessAuthority;
    }




}
