package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
//        test();
//        search();
//        collect();
        test1();
    }

    public static void test() throws IOException {
        BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/java/corpus.jsonl.json"));
        ObjectMapper objectMapper = new ObjectMapper();

        List<Document> documentList = new ArrayList<>();


        String line;
        while((line = bufferedReader.readLine()) != null){
            Document document = new Document();
            JsonNode jsonNode = objectMapper.readTree(line);
            document.add(new TextField("id", jsonNode.get("_id").toString(), Field.Store.YES));
            document.add(new TextField("title", jsonNode.get("title").toString(), Field.Store.YES));
            document.add(new TextField("text", jsonNode.get("text").toString(), Field.Store.YES));
            document.add(new TextField("metadata", jsonNode.get("metadata").toString(), Field.Store.YES));

            documentList.add(document);
        }

        Analyzer analyzer = new StandardAnalyzer();

        Directory directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(directory, config);

        for(Document doc: documentList){
            writer.addDocument(doc);
        }

        writer.close();
        bufferedReader.close();
    }

    public static List<Querry> collect() throws IOException {
        BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/java/queries.jsonl.json"));
        ObjectMapper objectMapper = new ObjectMapper();
        String line;
        List<Querry> querries = new LinkedList<>();
        while((line = bufferedReader.readLine()) != null){
            JsonNode jsonNode = objectMapper.readTree(line);
            Querry query = new Querry(Integer.parseInt(jsonNode.get("_id").toString().replace("\"", "")), jsonNode.get("text").toString());
            querries.add(query);
        }
        bufferedReader.close();
        return querries;
    }

    public static void test1() throws IOException {
        List<Querry> collect = collect();
        System.out.println(collect.size());
        searchQuerry(collect);
    }

    public static void searchQuerry(List<Querry> querries) throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser queryParser = new QueryParser("text", analyzer);
        Directory directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        Query query;
        try {
            for(Querry querry: querries){
                System.out.println(querry);
                // Escape all the characters
                query = queryParser.parse(QueryParser.escape(querry.getText()));
                TopDocs search = indexSearcher.search(query, 1);
                ScoreDoc[] scoreDocs = search.scoreDocs;
                if(scoreDocs != null){
                    for(ScoreDoc scoreDoc: scoreDocs){
                        int doc = scoreDoc.doc;
                        Document document = indexSearcher.storedFields().document(doc);
                        System.out.println(document);
                    }
                }else{
                    System.out.println("Score Doc is empty");
                }
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        indexReader.close();
    }

    public static void search() throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser queryParser = new QueryParser("text", analyzer);
        Query query;
        try {
            query = queryParser.parse("0-dimensional biomaterials lack inductive properties.");
            System.out.println(query);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        Directory directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        TopDocs search = indexSearcher.search(query, 10);
        ScoreDoc[] scoreDocs = search.scoreDocs;
        System.out.println("总数: " + search.totalHits);
        if(scoreDocs != null){
            for(ScoreDoc scoreDoc: scoreDocs){
                int doc = scoreDoc.doc;
                Document document = indexSearcher.storedFields().document(doc);
                System.out.println(document);
            }
        }else{
            System.out.println("Score Doc is empty");
        }

        indexReader.close();
    }
}