package com.sbn.italianref.Handlers;

import com.google.gson.JsonObject;
import com.sbn.italianref.ProcessText;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class LuceneHandler {


    private static int documentsProcessed = 0;
    public static Path luceneIndexPath;
    private static Analyzer analyzer;


    private static IndexWriter openIndex() {
        try {
            Directory dir = FSDirectory.open(luceneIndexPath);
            IndexWriterConfig iwc = new IndexWriterConfig(getAnalyzer());
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            IndexWriter indexWriter = new IndexWriter(dir, iwc);
            return indexWriter;
        } catch (Exception e) {
            System.err.println("Error opening the index: " + e.getMessage());
            return null;

        }
    }

    public static IndexReader getIndexReader() throws IOException {
        Directory dir = FSDirectory.open(luceneIndexPath);
        IndexReader ir = DirectoryReader.open(dir);
        return ir;
    }

    public static int indexTweets(Stream<JsonObject> tweets) {
        try {
            IndexWriter iw = openIndex();
            ProcessText processor = new ProcessText();
            tweets.forEach((tweet) -> { addDocumentToIndex(iw, tweet); });
            iw.commit();
            iw.close();
            return documentsProcessed;
        } catch (IOException e) {
            System.err.println("Error writing to index: " + e.getMessage());
            System.exit(0);
            return 0;
        }
    }

    public static Analyzer getAnalyzer() throws IOException {
        if(analyzer == null) {
            CharArraySet stopWords = ItalianAnalyzer.getDefaultStopSet();
            ResourcesHandler rh = new ResourcesHandler();
            Path stopWordsItPath = rh.getDirFromResources("stopwords-it.txt");
            Scanner scanner = new Scanner(stopWordsItPath);
            while(scanner.hasNextLine()) {
                stopWords.add(scanner.nextLine());
            }
            StandardAnalyzer standardAnalyzer = new StandardAnalyzer(stopWords);
            WhitespaceAnalyzer whitespaceAnalyzer = new WhitespaceAnalyzer();
            Map<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();
            analyzerPerField.put("user", whitespaceAnalyzer);
            analyzerPerField.put("user_created_tweet", whitespaceAnalyzer);
            analyzerPerField.put("original_text", whitespaceAnalyzer);
            analyzerPerField.put("text", standardAnalyzer);
            analyzer = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(), analyzerPerField);
        }
        return analyzer;

    }

    public static Map<String, Long> getTermVector(IndexReader ir, int docId, String field) throws IOException {
        Terms termVector = ir.getTermVector(docId, field);
        Map<String, Long> terms = new HashMap<String, Long>();
        if(termVector != null) {
            TermsEnum termsEnum = termVector.iterator();
            while (termsEnum.next() != null) {
                String term = termsEnum.term().utf8ToString();
                long toAdd = terms.containsKey(term) ? terms.get(term) + 1 : 1;
                terms.put(term, toAdd);
            }
        }
        return terms;
    }

    private static void addDocumentToIndex(IndexWriter iw, JsonObject tweet) {
        documentsProcessed += 1;
        Document doc = new Document();
        for(String key : tweet.keySet()) {
            if(key.equals("created_at")){
                doc.add(new StoredField(key, tweet.get(key).getAsLong()));
                doc.add(new LongPoint(key+"_range", tweet.get(key).getAsLong()));
            }
            else if(key.equals("text")) {
                FieldType fieldType = new FieldType();
                fieldType.setStored(true);
                fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
                fieldType.setStoreTermVectors(true);
                String originalText = tweet.get(key).getAsString();
                String resultString = ProcessText.processText(originalText);
                doc.add(new Field(key, resultString, fieldType));
                FieldType otherFieldType = StringField.TYPE_STORED;
                doc.add(new Field("original_text", originalText, otherFieldType));
            } else {
                FieldType fieldType = StringField.TYPE_STORED;
                doc.add(new Field(key, tweet.get(key).getAsString(), fieldType));
            }
        }
        try {
            iw.addDocument(doc);
        } catch (IOException e) {
            System.err.println("Error writing to index: " + e.getMessage());
            System.exit(0);
        }
    }

    public static Map<Integer, Document> searchIndex(List<String> field, String query) throws IOException {
        try {
            Directory dir = FSDirectory.open(luceneIndexPath);
            IndexReader ir = DirectoryReader.open(dir);
            IndexSearcher is = new IndexSearcher(ir);
            Query luceneQuery = new MultiFieldQueryParser(field.toArray(new String[field.size()]), getAnalyzer()).parse(query);
            int maxDocs = ir.numDocs();
            TopDocs docs = is.search(luceneQuery, maxDocs);
            Map<Integer, Document> documents = Arrays
                    .stream(docs.scoreDocs)
                    .collect(Collectors.toMap(scoreDoc -> scoreDoc.doc, scoreDoc -> {
                        try {
                            return is.doc(scoreDoc.doc);
                        } catch (IOException e) {
                            e.printStackTrace();
                            return null;
                        }
                    }));
            return documents;
        } catch (Exception e) {
            System.err.println("Error querying index: " +e.getMessage());
            System.exit(0);
            return null;
        }

    }

    public static Map<String, Long> getTermsFrequency(String field, String query) throws IOException {
        try {
            Directory dir = FSDirectory.open(luceneIndexPath);
            IndexReader ir = DirectoryReader.open(dir);
            IndexSearcher is = new IndexSearcher(ir);
            Query luceneQuery = new QueryParser(field, getAnalyzer()).parse(query);
            int maxDocs = ir.numDocs();
            TopDocs docs = is.search(luceneQuery, maxDocs);
            int[] docIds = Arrays.stream(docs.scoreDocs).mapToInt(x -> x.doc).toArray();
            Map<String, Long> termsMap = getTermsFrequencyByDocIds(ir, docIds);
           return termsMap;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error querying index: " + e.getMessage());
            System.exit(0);
            return null;
        }
    }

    public static Map<String, Long> getTermsFrequencyByDocIds(IndexReader ir, int []docIds) {
        try {
            Map<String, Long> termsMap = new HashMap<String, Long>();
            for(int docId : docIds) {
                Terms terms = ir.getTermVector(docId, "text");
                if(terms != null) {
                    TermsEnum termsEnum = terms.iterator();
                    while (termsEnum.next() != null) {
                        String text = termsEnum.term().utf8ToString();
                        long freq = termsEnum.totalTermFreq();
                        if (termsMap.containsKey(text)) {
                            termsMap.put(text, termsMap.get(text) + freq);
                        } else {
                            termsMap.put(text, freq);
                        }
                    }
                }
            }
            return termsMap;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error querying index: " + e.getMessage());
            System.exit(0);
            return null;
        }
    }

}
