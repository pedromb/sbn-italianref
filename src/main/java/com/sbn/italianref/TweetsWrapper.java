package com.sbn.italianref;

import com.google.gson.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class TweetsWrapper {

    private Path streamPath;
    private int limitTweets;
    private final static Matcher m = Pattern.compile("^<o><t>([0-9]+)</t><l>([a-zA-Z]+.[a-zA-Z]*)</l><p><!\\[CDATA\\[(.*)]]></p></o>").matcher("");
    private final static JsonParser jp = new JsonParser();


    public TweetsWrapper(Path streamPath, int limitTweets) {
        this.streamPath = streamPath;
        this.limitTweets = limitTweets;
    }

    public void indexTweetsToLucene() {
        this.processTweets();
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
            String oldMessage = "Processing files: 0.00% | Time Elapsed; 0m0s | Time Remaining: tbd";
            System.out.print(oldMessage);
            Instant start = Instant.now();
            for (File file : files) {
                Stream<JsonObject> tweets = this.readFile(file);
                int numTweetsIndexed = LuceneWrapper.indexTweets(tweets);
                Instant end = Instant.now();
                long duration = Duration.between(start, end).getSeconds();
                filesProcessed += 1;
                oldMessage = this.progressPct(oldMessage, filesProcessed, totalNumOfFiles, duration);
                if (this.limitTweets != 0 && numTweetsIndexed > this.limitTweets) break;
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    private Stream readFile(File file) {
        try {
            List<JsonObject> tweets = new ArrayList<JsonObject>();
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            Stream<String> fileLines = br.lines();
            return fileLines.map(this::getRawJson);
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
            String lang = m.group(2);
            String rawJson= m.group(3);
            JsonObject jo = jp.parse(rawJson).getAsJsonObject();
            return jo;
        }
        return null;
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
