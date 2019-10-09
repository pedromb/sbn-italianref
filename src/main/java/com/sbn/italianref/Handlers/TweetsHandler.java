package com.sbn.italianref.Handlers;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sbn.italianref.Models.TweetModel;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class TweetsHandler {

    private Path streamPath;
    private int limitTweets;
    private final static Matcher m = Pattern.compile("^<o><t>([0-9]+)</t><l>([a-zA-Z]+.[a-zA-Z]*)</l><p><!\\[CDATA\\[(.*)]]></p></o>").matcher("");
    private final static JsonParser jp = new JsonParser();


    public TweetsHandler(Path streamPath, int limitTweets) {
        this.streamPath = streamPath;
        this.limitTweets = limitTweets;
    }

    public void indexTweetsToLucene() {
        this.processTweets();
    }

    public List<TweetModel> queryTweets(List<String> field, String query) throws IOException {
        Map <Integer, Document> tweets = LuceneHandler.searchIndex(field, query);
        IndexReader ir = LuceneHandler.getIndexReader();
        List<TweetModel> tweetsModel = new ArrayList<TweetModel>();
        for(int tweetId: tweets.keySet()) {
            TweetModel tweetModel = new TweetModel();
            tweetModel.setUser(tweets.get(tweetId).get("user"));
            tweetModel.setUserCreatedTweet(tweets.get(tweetId).get("user_created_tweet"));
            long createdAtTimestamp = Long.parseLong(tweets.get(tweetId).get("created_at"));
            tweetModel.setCreatedAtTimestamp(createdAtTimestamp);
            LocalDateTime createdAt = new Timestamp(createdAtTimestamp).toLocalDateTime();
            tweetModel.setCreatedAt(createdAt);
            tweetModel.setText(tweets.get(tweetId).get("text"));
            tweetModel.setDocId(tweetId);
            tweetModel.setUserId(tweets.get(tweetId).get("user_id"));
            tweetModel.setTermsVector(LuceneHandler.getTermVector(ir, tweetId, "text"));
            tweetsModel.add(tweetModel);
        }
        return tweetsModel;
    }

    public Map<String, Long> getTermsFrequency(String field, String query, int topN) throws IOException {
        Map<String, Long> termsFrequency = LuceneHandler.getTermsFrequency(field, query);
        Map<String, Long> termsFrequencyLimited = termsFrequency
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(topN)
                .collect(Collectors.toMap(x->x.getKey(), x->x.getValue()));
        return termsFrequencyLimited;
    }

    public Map<String, Long> getTermsFrequencyByDocIds(int [] docIds) throws IOException {
        IndexReader ir = LuceneHandler.getIndexReader();
        return LuceneHandler.getTermsFrequencyByDocIds(ir, docIds);
    }

    public static List<TweetModel> addSupportToTweetModel(List<TweetModel> tweets,  HashMap<String, String> users) {

        for(TweetModel tweet : tweets) {
            String userSupport = users.containsKey(tweet.getUser()) ? users.get(tweet.getUser()) : "none";
            tweet.setUserSupport(userSupport);
            String userCreatedTweetSupport = users.containsKey(tweet.getUserCreatedTweet()) ? users.get(tweet.getUserCreatedTweet()) : "none";
            tweet.setUserCreatedTweetSupport(userCreatedTweetSupport);
            String actualSupport = tweet.getUserSupport().equals("none") ? tweet.getUserCreatedTweetSupport() : tweet.getUserSupport();
            tweet.setActualSupport(actualSupport);
        }
        return tweets;
    }

    private void processTweets() {
        try {
            int tweetsProcessed = 0;
            List<File> files = Files.walk(this.streamPath)
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .filter((file -> !file.toString().contains(".DS_Store")))
                    .collect(Collectors.toList());
            int totalNumOfFiles = files.size();
            int filesProcessed = 0;

            System.out.println("\nIndexing Data on Lucene");
            String oldMessage = "Processing files: 0.00% | Time Elapsed: 0m0s | Time Remaining: tbd";
            System.out.print(oldMessage);
            Instant start = Instant.now();
            for (File file : files) {
                Stream<JsonObject> tweets = this.readFile(file);
                int numTweetsIndexed = LuceneHandler.indexTweets(tweets);
                Instant end = Instant.now();
                long duration = Duration.between(start, end).getSeconds();
                filesProcessed += 1;
                oldMessage = this.progressPct(oldMessage, filesProcessed, totalNumOfFiles, duration);
                if (this.limitTweets != 0 && numTweetsIndexed > this.limitTweets) break;
            }
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Stream readFile(File file) {
        try {
            List<JsonObject> tweets = new ArrayList<JsonObject>();
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            Stream<String> fileLines = br.lines();
            return fileLines
                    .map(this::getRawJson)
                    .map(this::subsetRawJson);
        } catch (Exception e) {
            System.err.println("Error: File reading file: " + e.getMessage());
            System.exit(0);
            return null;
        }

    }

    private JsonObject getRawJson(String line) {
        m.reset(line);
        if (m.find()) {
            Long time = Long.parseLong(m.group(1));
            String rawJson= m.group(3);
            JsonObject jo = jp.parse(rawJson).getAsJsonObject();
            SimpleDateFormat datetimeFormatter1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
            try {
                Date lFromDate1 = datetimeFormatter1.parse(jo.get("created_at").getAsString());
                jo.addProperty("timestamp", lFromDate1.getTime());
            } catch (Exception e) {
                jo.addProperty("timestamp", time);
            }
            return jo;
        }
        return null;
    }

    private JsonObject subsetRawJson(JsonObject originalJson) {
        JsonObject newJson = new JsonObject();
        JsonObject user = originalJson.getAsJsonObject("user");
        String userId = user.get("id_str").getAsString();
        String user_name = user.get("screen_name").getAsString();
        String text = originalJson.get("text").getAsString();
        String userCreatedTweet = user_name;
        String originalTweetId = originalJson.get("id_str").getAsString();
        boolean isRetweet = originalJson.has("retweeted_status");
        if(isRetweet) {
            text = originalJson
                .getAsJsonObject("retweeted_status")
                .get("text")
                .getAsString();
            userCreatedTweet = originalJson
                .getAsJsonObject("retweeted_status")
                .getAsJsonObject("user")
                .get("screen_name")
                .getAsString();
            originalTweetId = originalJson
                .getAsJsonObject("retweeted_status")
                .get("id_str")
                .getAsString();
        }
        newJson.addProperty("user", user_name);
        newJson.addProperty("user_created_tweet", userCreatedTweet);
        newJson.addProperty("text", text);
        newJson.addProperty("created_at", originalJson.get("timestamp").getAsLong());
        newJson.addProperty("id", originalJson.get("id_str").getAsString());
        newJson.addProperty("original_tweet_id", originalTweetId);
        newJson.addProperty("is_retweet", isRetweet);
        newJson.addProperty("user_id", userId);

        return newJson;
    }

    private String progressPct(String oldMessage, int num, int total, long durationSeconds) {
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
        String processingFiles =  "Processing files: "+df.format(pct)+"%";
        String elapsed = "Time Elapsed: "+elapsedMinutes+"m"+elapsedSeconds+"s";
        String remaining = "Time Remaining: "+remainingMinutes+"m"+remainingSeconds+"s";
        String newMessage = processingFiles+" | "+elapsed+" | "+remaining;
        System.out.print(backspaces);
        System.out.print(newMessage);
        return newMessage;
    }


}
