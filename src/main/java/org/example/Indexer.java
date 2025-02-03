package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class Indexer {

    // Directory for the indices
    private Directory directory;

    // Reader to reade the json files
    private BufferedReader reader;

    // Retrieve the json file
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Stop words
    private static final CharArraySet ENGLISH_STOP_WORDS_SET = getStopWords();

    // Tokenizer
    private final Analyzer analyzer = new EnglishAnalyzer(ENGLISH_STOP_WORDS_SET);

    // To build inverted index
    private IndexWriter writer;

    public Indexer(){
        initializeIndexer();
    }

    private void initializeIndexer(){
        try{
            this.directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
            // Let the reader ready to read the corpus file
            this.reader = Files.newBufferedReader(Paths.get("src/main/java/corpus.jsonl.json"));
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            this.writer = new IndexWriter(directory, config);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void index(){
        // 1. Prepare documents
        try {
            // Read each line
            String line;
            while ((line = reader.readLine()) != null) {
                Document document = new Document();
                // Customize store options
                FieldType customType = new FieldType();
                customType.setStored(true);
                customType.setTokenized(true);
                customType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
                // Must store the term vectors for calculating tf_idf value
                customType.setStoreTermVectors(true);
                customType.setStoreTermVectorPositions(true);
                customType.setStoreTermVectorOffsets(true);
                customType.freeze();

                // Retrieve
                JsonNode jsonNode = objectMapper.readTree(line);
                // Convert each field into Lucene's field
                document.add(new StringField("id", jsonNode.get("_id").toString(), Field.Store.YES));
                // Let title and text field share the same tokenizer
                // TODO: 测试组合查询
                /*
                public void search(String queryText) throws IOException {
                    Query titleQuery = new TermQuery(new Term("title", queryText));
                    Query textQuery = new TermQuery(new Term("text", queryText));
                 */
//                document.add(new Field("content",
//                        jsonNode.get("title").asText()
//                                + " "
//                                + jsonNode.get("text").asText(), customType));
                document.add(new Field("title", jsonNode.get("title").asText(), customType));
                document.add(new Field("text", jsonNode.get("text").asText(), customType));
                document.add(new TextField("metadata", jsonNode.get("metadata").asText(), Field.Store.YES));

                // 3. Indexing
                writer.addDocument(document);
            }
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            closeAll();
        }
    }

    public Analyzer getAnalyzer(){
        return this.analyzer;
    }

    private void closeAll(){
        try {
            this.reader.close();
            this.writer.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    // Get the stop words from the file
    public static CharArraySet getStopWords(){
        try (BufferedReader reader = Files.newBufferedReader(Paths.get("src/main/java/stop_words.txt"))) {
            List<String> stopwords = new LinkedList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                stopwords.add(line.trim());
            }
            return CharArraySet.unmodifiableSet(new CharArraySet(stopwords, false));
        } catch (IOException e) {
            e.printStackTrace();
            return CharArraySet.EMPTY_SET;
        }
    }
}
