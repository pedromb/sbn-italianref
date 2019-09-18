package com.sbn.italianref;

import com.google.gson.JsonObject;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class LuceneWrapper {


    private static int documentsProcessed = 0;

    private static IndexWriter openIndex(Path indexPath) {
        try {
/*
            InputStream stopWords = new FileInputStream("teste");
*/
/*
            Reader readerStopWords = new InputStreamReader(stopWords);
*/
            Directory dir = FSDirectory.open(indexPath);
            StandardAnalyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            IndexWriter indexWriter = new IndexWriter(dir, iwc);
            return indexWriter;
        } catch (Exception e) {
            System.err.println("Error opening the index: " + e.getMessage());
            return null;

        }
    }

    public static int indexTweets(Path indexPath, Stream<JsonObject> tweets) {
        try {
            IndexWriter iw = openIndex(indexPath);
            tweets.forEach((tweet) -> addDocumentToIndex(iw, tweet));
            iw.commit();
            iw.close();
            return documentsProcessed;
        } catch (IOException e) {
            System.err.println("Error writing to index: " + e.getMessage());
            System.exit(0);
            return 0;
        }
    }

    private static void addDocumentToIndex(IndexWriter iw, JsonObject tweet) {
        documentsProcessed += 1;
        Document doc = new Document();
        doc.add(new Field("user",  tweet.get("user").getAsJsonObject().get("screen_name").getAsString(), TextField.TYPE_STORED));
        doc.add(new Field("timestamp",  tweet.get("created_at").getAsString(), StringField.TYPE_STORED));
        doc.add(new Field("text",  tweet.get("text").getAsString(), StringField.TYPE_STORED));
        try {
            iw.addDocument(doc);
        } catch (IOException e) {
            System.err.println("Error writing to index: " + e.getMessage());
            System.exit(0);
        }
    }
}
