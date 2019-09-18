package com.sbn.italianref;


import java.nio.file.Path;
import java.nio.file.Paths;


public class App {

    private static String STREAM_RESOURCE = "stream";
    private static String USERS_TO_FILTER_RESOURCE = "users.csv";
    private static String LUCENE_INDEX = "lucene_index/";
    private static int TWEETS_LIMIT = 0;

    public static void main(String[] args) throws Exception {
        ResourcesHandler rh = new ResourcesHandler();
        Path streamPath = rh.getDirFromResources(STREAM_RESOURCE);
        Path luceneIndexPath = Paths.get(LUCENE_INDEX);
        TweetsWrapper tw = new TweetsWrapper(streamPath, TWEETS_LIMIT);
        tw.indexTweetsToLucene(luceneIndexPath);
    }
}