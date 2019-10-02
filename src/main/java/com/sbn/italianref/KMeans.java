package com.sbn.italianref;

import com.sbn.italianref.Models.TermsTimeSeriesModel;
import net.seninp.jmotif.sax.SAXException;
import net.seninp.jmotif.sax.SAXProcessor;

import java.util.*;
import java.util.stream.Collectors;

public class KMeans {


    public static Map<String, List<String>> fitForSax(
            Map<String, TermsTimeSeriesModel> entries,
            int k,
            double[][] distanceMatrix,
            int alphabetSize,
            int maxIterations
    ) throws SAXException {

        int numIterationsToConverge = 0;
        TermsTimeSeriesModel firstTerm = (TermsTimeSeriesModel) entries.values().toArray()[0];
        int saxStringSize = firstTerm.getSax().length();
        SAXProcessor saxProcessor = new SAXProcessor();
        List<String> centroids = initCentroids(k, alphabetSize, saxStringSize, entries, distanceMatrix, saxProcessor);
        HashMap<String, List<String>> clusters = new HashMap<>();
        for(int i = 0; i<maxIterations; i++) {
            HashMap<String, List<String>> previousClusters = new HashMap<String, List<String>>();
            if(clusters != null) {
                previousClusters.putAll(clusters);
            }
            clusters = calculateClusters(entries, distanceMatrix, saxProcessor, centroids);
            centroids = updateCentroids(entries, clusters);
            numIterationsToConverge += 1;
            if(checkStopCondition(previousClusters, clusters)) {
                break;
            }
        }
        System.out.print("KMeans took "+numIterationsToConverge+" iterations to converge...");
        return clusters;
    }


    public static double saxDistance(
            TermsTimeSeriesModel a, String b,
            double[][] distanceMatrix, SAXProcessor saxProcessor) throws SAXException {

        char[] aString = a.getSax().toCharArray();
        char[] bString = b.toCharArray();
        double distance = saxProcessor.saxMinDist(
                aString, bString, distanceMatrix, a.getOriginalTs().length, a.getW()
        );
        return distance;
    }

    private static List<String> initCentroids(
            int k,
            int alphabetSize,
            int saxStringSize,
            Map<String, TermsTimeSeriesModel> entries,
            double[][] distanceMatrix,
            SAXProcessor saxProcessor
    ) throws SAXException {
        char[] ALPHABET = new char[]{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
        Map<List<String>, Map<String, List<Double>>> centroidsOptions = new HashMap<>();
        for(int z = 0; z < 100; z++) {
            List<String> currentCentroids = new ArrayList<String>();
            Random randomizer = new Random(z);
            for (int i = 0; i < k; i++) {
                String newCentroid = "";
                for (int j = 0; j < saxStringSize; j++) {
                    newCentroid += ALPHABET[randomizer.nextInt(alphabetSize)];
                }
                currentCentroids.add(newCentroid);
            }
            Map<String, List<Double>> clusterAvgDist = getClustersWithAvgDistances(entries, distanceMatrix, saxProcessor, currentCentroids);
            clusterAvgDist = clusterAvgDist
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            x->x.getKey(),
                            x-> {
                                List<Double> newValue = new ArrayList<>();
                                newValue.add(x.getValue().stream().mapToDouble(Double::doubleValue).average().getAsDouble());
                                newValue.add((double) x.getValue().size());
                                return newValue;
                            }))
                    .entrySet()
                    .stream()
                    .filter(x -> x.getValue().get(1) > 200)
                    .collect(Collectors.toMap(x->x.getKey(), x->x.getValue()));
            if (clusterAvgDist.size() == k) centroidsOptions.put(currentCentroids, clusterAvgDist);
        }

        List<String> chosenCentroids = null;
        double min = Double.POSITIVE_INFINITY;
        for(List<String> centroids: centroidsOptions.keySet()) {
            double avg = 0;
            for( String centroidInfo : centroidsOptions.get(centroids).keySet()) {
                avg += centroidsOptions.get(centroids).get(centroidInfo).get(0);
            }
            avg /= centroidsOptions.get(centroids).size();
            if(avg < min) {
                min = avg;
                chosenCentroids = centroids;
            }
        }
        return chosenCentroids;
    }

    private static List<String> updateCentroids(
            Map<String, TermsTimeSeriesModel> entries,
            HashMap<String, List<String>> clusters
    ) {
        List<String> newCentroids = new ArrayList<String>();
        for(String centroid : clusters.keySet()) {
            List<char[]> membersChars = new ArrayList<>();
            List<String> centroidClusters = clusters.get(centroid);
            String newCent;
            if (centroidClusters.size() > 0) {
                for (String term : clusters.get(centroid)) {
                    char[] saxString = entries.get(term).getSax().toCharArray();
                    membersChars.add(saxString);
                }
                newCent = calculateMembersMean(membersChars);
            } else {
                newCent = centroid;
            }
            newCentroids.add(newCent);
        }
        return newCentroids;
    }

    private static String calculateMembersMean(List<char[]> membersChars) {
        int[] meanAscii = new int [membersChars.get(0).length];
        String meanChars = "";
        for(char [] member : membersChars) {
            for(int i = 0; i < member.length; i++) {
                meanAscii[i] += (int) member[i];
            }
        }
        for(int i = 0; i < meanAscii.length; i++) {
            meanChars += (char) ((meanAscii[i] / membersChars.size()));
        }
        return meanChars;
    }

    private static HashMap<String, List<String>> calculateClusters(
            Map<String, TermsTimeSeriesModel> entries,
            double[][] distanceMatrix,
            SAXProcessor saxProcessor,
            List<String> centroids
    ) throws SAXException {
        HashMap<String, List<String>> newClusters = new HashMap<String, List<String>>();
        for (String term : entries.keySet()) {
            TermsTimeSeriesModel a = entries.get(term);
            double min = Double.POSITIVE_INFINITY;
            String minCentroid = "";
            for (String centroid : centroids) {
                double dist = saxDistance(a, centroid, distanceMatrix, saxProcessor);
                if (dist < min) {
                    min = dist;
                    minCentroid = centroid;
                }
            }
            if (newClusters.containsKey(minCentroid)) {
                newClusters.get(minCentroid).add(term);
            } else {
                List <String> newEntry = new ArrayList<String>();
                newEntry.add(term);
                newClusters.put(minCentroid, newEntry);
            }

        }
        return newClusters;
    }

    private static Map<String, List<Double>> getClustersWithAvgDistances(
            Map<String, TermsTimeSeriesModel> entries,
            double[][] distanceMatrix,
            SAXProcessor saxProcessor,
            List<String> centroids
    ) throws SAXException {
        HashMap<String, List<Double>> newClusters = new HashMap<String, List<Double>>();
        for (String term : entries.keySet()) {
            TermsTimeSeriesModel a = entries.get(term);
            double min = Double.POSITIVE_INFINITY;
            String minCentroid = "";
            for (String centroid : centroids) {
                double dist = saxDistance(a, centroid, distanceMatrix, saxProcessor);
                if (dist < min) {
                    min = dist;
                    minCentroid = centroid;
                }
            }
            if (newClusters.containsKey(minCentroid)) {
                newClusters.get(minCentroid).add(min);
            } else {
                List <Double> newEntry = new ArrayList<Double>();
                newEntry.add(min);
                newClusters.put(minCentroid, newEntry);
            }
        }
        return newClusters;
    }


    private static boolean checkStopCondition(
            HashMap<String, List<String>> previousClusters,
            HashMap<String, List<String>> clusters
    ) {
        if(previousClusters == null) {return false;}
        for(String centroid : clusters.keySet()) {
            if(!previousClusters.containsKey(centroid)) {
                return false;
            } else {
                List<String> previousList = previousClusters.get(centroid);
                Collections.sort(previousList);
                List<String> currentList = clusters.get(centroid);
                Collections.sort(currentList);
                if(!previousList.equals(currentList)) {
                    return false;
                }

            }
        }
        return true;
    }


}
