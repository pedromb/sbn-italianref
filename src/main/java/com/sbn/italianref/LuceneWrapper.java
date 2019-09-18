package com.sbn.italianref;

import com.google.gson.JsonObject;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LuceneWrapper {


    private static int documentsProcessed = 0;
    public static Path luceneIndexPath;

    private static IndexWriter openIndex() {
        try {
/*
            InputStream stopWords = new FileInputStream("teste");
*/
/*
            Reader readerStopWords = new InputStreamReader(stopWords);
*/
            Directory dir = FSDirectory.open(luceneIndexPath);
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

    public static int indexTweets(Stream<JsonObject> tweets) {
        try {
            IndexWriter iw = openIndex();
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

    public static List<Document> searchIndex(String field, String query) throws IOException {
        try {
            StandardAnalyzer analyzer = new StandardAnalyzer();
            Directory dir = FSDirectory.open(luceneIndexPath);
            IndexReader ir = DirectoryReader.open(dir);
            IndexSearcher is = new IndexSearcher(ir);
            Query luceneQuery = new QueryParser(field, analyzer).parse(query);
            int maxDocs = ir.numDocs();
            TopDocs docs = is.search(luceneQuery, maxDocs);
            return Arrays.stream(docs.scoreDocs)
                    .map(scoreDoc -> {
                        try {
                            return is.doc(scoreDoc.doc);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return null;
                        }
                    })
                    .collect(Collectors.toList());
        } catch (Exception e) {
            System.err.println("Error querying index: " +e.getMessage());
            System.exit(0);
            return null;
        }

    }



}
