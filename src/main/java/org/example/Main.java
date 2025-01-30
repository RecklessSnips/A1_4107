package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        test();
//        test1();
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

    public static void test1() throws IOException {
        BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/java/queries.jsonl.json"));
        String line;
        int count = 0;
        while((line = bufferedReader.readLine()) != null){
            count += 1;
            System.out.println(line);
        }
        System.out.println(count);
        bufferedReader.close();
    }
}