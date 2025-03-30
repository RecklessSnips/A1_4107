package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Vector {

    // To calculate tf, idf
    private final ClassicSimilarity similarity = new ClassicSimilarity();

    // Read inverted index
    private IndexReader indexReader;

    // Same analyzer as the Indexer class
    private Analyzer analyzer;

    private Map<String, String> corpusList;

    // Store the document vector
    private Map<Integer, Map<String, Double>> documentVector;

    // Store the query vector
    private Map<String, Double> queryVector;

    // Total number of documents: 5183
    private final int TOTALDOCS;

    // Writer
    private BufferedWriter writer;

    public Vector(Analyzer analyzer, Map<String, String> corpusList){
        initialize();
        documentVector = new HashMap<>();
        queryVector = new HashMap<>();
        TOTALDOCS = indexReader.numDocs();
        this.analyzer = analyzer;
        this.corpusList = corpusList;
    }

    public void initialize(){
        try {
            Directory directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
            // Prepare reader
            indexReader = DirectoryReader.open(directory);
        }catch (IOException e){
            System.out.println("Error while opening file");
        }

        try{
            writer = Files.newBufferedWriter(Paths.get("src/main/java/results.csv"));
        }catch (IOException e){
            System.out.println("Error while opening writer, did you forget to specify the file name?");
        }
    }

    /**
        Method to build the document vector for each document
        @param field Specify the which (Lucene) field to construct the vector
        @return The map of documents, where the key is each document id, and the value is
                the weight of each term ( tf_idf(t, d) )
    */
    public Map<Integer, Map<String, Double>> buildDocumentVector(String field){
        for (int i = 0; i < TOTALDOCS; i++) {
            Map<String, Double> termsWeight = buildDocumentVectorHelper(i, field);
            documentVector.put(i, termsWeight);
        }
        return documentVector;
    }

    /**
     * Helper method to construct document vector for each document
     * @param docID Specify which document vector is being built
     * @param field The field to build con
     * @return The map of each term and its tf_idf value
     * @throws IOException if the index didn't store the term vectors
     */
    private Map<String, Double> buildDocumentVectorHelper(int docID, String field){
        try {
            // Get all term vectors
            TermVectors termVectors = indexReader.termVectors();
            // Get all the terms in this document
            Terms terms = termVectors.get(docID, field);
            if(terms == null){
                return Collections.emptyMap();
            }
            // Instantiate the map to store each term's tf_idf
            Map<String, Double> tfIdfMap = new HashMap<>();
            TermsEnum iterator = terms.iterator();
            // Loop each term
            while (iterator.next() != null){
                // Get the text
                String s = iterator.term().utf8ToString();
                // Calculate the term frequency tf(t)
                long frequency = iterator.totalTermFreq();
                // Calculate the document frequency Df(t)
                int df = indexReader.docFreq(new Term(field, s));
                // Calculate idf
                double idf = similarity.idf(df, TOTALDOCS);
                // Calculate tf_idf
                double tf_idf = frequency * idf;

                // Create Document vector, later will put into the document vector map
                tfIdfMap.put(s, tf_idf);
            }

            // Optimization: normalize the vector so the term stays informative in long text
            Map<String, Double> stringDoubleMap = normalizeVector(tfIdfMap);
            // Clean the old map
            tfIdfMap.clear();
            tfIdfMap.putAll(stringDoubleMap);
            return tfIdfMap;
        }catch (IOException e){
            return Collections.emptyMap();
        }
    }

    /**
     * Method to build a query vector
     * @param query The query to be tokenized
     * @param field The field to calculate the document frequency and searching
     * @return The map of each query term and their tf_idf value
     */
    public Map<String, Double> buildQueryVector(String query, String field){
        try {
            // Retrieve tokenized query
            List<String> tokens = tokenizeQuery(query);
            System.out.println("Query: " + query);

            // Calculate each token's tf_idf
            for(String token: tokens){
                // Calculate tf(t, q)
                long frequency = tokens.stream().filter(t -> t.equals(token)).count();

                // Calculate Df(t)
                int df = indexReader.docFreq(new Term(field, token));
                // Calculate idf
                double idf = similarity.idf(df, TOTALDOCS);

                // Calculate tf_idf
                double tf_idf = (frequency) * idf;
                queryVector.put(token, tf_idf);
            }

            // Optimization: normalize the vector so the term stays informative in long text
            Map<String, Double> stringDoubleMap = normalizeVector(queryVector);
            // Store in a separate map to be returned
            Map<String, Double> result = new HashMap<>(stringDoubleMap);
            // Clear the map for next round calculation
            queryVector.clear();
            return result;
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    /**
     * Method for tokenizing query
     * @param query The query to be tokenized
     * @return the tokenized query
     */
    private List<String> tokenizeQuery(String query){
        // Get the token stream of the analyzer
        try(TokenStream tokenStream
                    = analyzer.tokenStream(null, new StringReader(query))){
            List<String> tokens = new LinkedList<>();
            // Add the char term attribute to the stream
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()){
                // Add the token
                tokens.add(charTermAttribute.toString());
            }
            tokenStream.end();
            return tokens;
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Method to compute the Cosine Similarity between each document and the query
     * @param documentVector the document vector of the corpus
     * @param queryVector the query vector to compute with
     * @return the sorted Cosine Similarity map where the key is document id and the value is the similarity of the document
     */
    public LinkedHashMap<Integer, Double> computeCosineSimilarity(Map<Integer, Map<String, Double>> documentVector,
                                        Map<String, Double> queryVector){
        // To be returned
        Map<Integer, Double> similarities = new HashMap<>();
        // Loop through the document vectors to compute
        for(Map.Entry<Integer, Map<String, Double>> mapEntry: documentVector.entrySet()){
            // Get each document's document vector
            Map<String, Double> value = mapEntry.getValue();
            // Compute
            double cosineSimilarity = cosineSimilarityHelper(value, queryVector);
            // Filter unrelated fields
            if(cosineSimilarity > 0.0) {
                // Add to the result
                similarities.put(mapEntry.getKey(), cosineSimilarity);
            }
        }
        LinkedHashMap<Integer, Double> sortedSimilarities = new LinkedHashMap<>();
        // Sort the docs in descending order
        similarities.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .forEachOrdered(entry -> sortedSimilarities.put(entry.getKey(), entry.getValue()));
        return sortedSimilarities;
    }

    /**
     * Helper method to compute a single document vector with the query vector
     * @param dv document vector
     * @param qv query vector
     * @return the Cosine Similarity between the vectors
     */
    public double cosineSimilarityHelper(Map<String, Double> dv, Map<String, Double> qv){
        double dotProduct = 0.0;
        // For later calculate the Euclidean length
        double euclideanLength_query = 0.0;
        double euclideanLength_document = 0.0;

        // Loop through the query vector to find the related fields
        for(Map.Entry<String, Double> entry : qv.entrySet()){
            String term = entry.getKey();
            double query_ifidf = entry.getValue();
            // If not in the dv, simply assign 0
            double document_tfidf = dv.getOrDefault(term, 0.0);
            dotProduct += query_ifidf * document_tfidf;
            // Euclidean length (without square root) for query vector
            euclideanLength_query += Math.pow(query_ifidf, 2);
        }

        // Euclidean length (without square root) for document vector
        for(Map.Entry<String, Double> entry : dv.entrySet()){
            double normalized_tfidf = entry.getValue();
            euclideanLength_document += Math.pow(normalized_tfidf, 2);
        }

        // Euclidean length
        double euclideanLength = Math.sqrt(euclideanLength_query)
                                * Math.sqrt(euclideanLength_document);

        // Cosine Similarity
        return dotProduct / euclideanLength;
    }

    /**
     * Perform the test on a single query from the corpus
     * @param query the query to be tested
     * @param dv document vector
     * @param limit indicates how many results to be showed (descending order)
     * @param ifClose false when running multiple queries together
     */
    public void runSingleQuery(Querry query, Map<Integer, Map<String, Double>> dv,
                               int limit, String field, boolean ifClose){
        // Build the qv for the current query
        Map<String, Double> qv = buildQueryVector(query.getText(), field);
        // Fetch the answer map from the csv file
        Map<String, String> answerSet = fetchAnswer();
        // Get the answer id for the current query
        String answerID = answerSet.get(String.valueOf(query.getId()));
        System.out.println("Answer ID: " + answerID);
        // Fetch all the corpus
        Map<String, String> answerMap = fetchCorpus();
        // Get the text from the answerID, this is the expected answer!
        String answer = answerMap.get(answerID);
        System.out.println("Answer: " + answer);

        // A counter to limit how many results to be shown (work with the parameter limit)
        int rank = 1;
        /*
         A counter to track the rank of the answer (if found),
         indicating the position of the answer in the sorted list displayed from highest to lowest similarity score.
         */
        int position = 0;
        boolean ifFound = false;
        // Sorted similarities
        LinkedHashMap<Integer, Double> similarities = computeCosineSimilarity(dv, qv);
        for (Map.Entry<Integer, Double> entry : similarities.entrySet()){
            if(rank > limit) break;
            position++;

            int docId = entry.getKey();
            double similarity = entry.getValue();
            try {
                Document doc = indexReader.storedFields().document(docId);
                String text = doc.get("text");
                if (text.equals(answer)){
                    System.out.println(similarity + ": " + text);
                    ifFound = true;
//                    System.out.println("Found!!!");
//                    break;
                }
                rank++;
                int tmp = rank;
                tmp--;
                writeResult(Integer.toString(query.getId()), corpusList.get(text),
                        tmp, similarity, "Keywords: " +
                                query.getText().substring(0, 5).replace("\"", "") + "...");
            } catch (IOException e) {
                throw new RuntimeException("There's a low-level IO error");
            }
        }
        if (position >= limit || position >= similarities.size()){
            System.out.println("Not found within the limit: " + limit);
        }else if (ifFound){
            System.out.println("Found in position: " + position);
        }
        if(ifClose){
            closeAll();
        }
    }

    /**
     * Method to run all the queries form the corpus
     */
    public void runQueriesOnField(String field){
        // Prepare the reader to read queries.json
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/java/queries.jsonl.json"))){
            ObjectMapper mapper = new ObjectMapper();
            String line;
            // Locked if the reader not yet reach the query with id = 1
            boolean lock = false;
            Map<Integer, Map<String, Double>> dv = buildDocumentVector(field);
            long start = System.currentTimeMillis();
            try{
                writer.write("query_id\t" + "Q0\t" + "doc_id\t" + "rank\t" + "score\t" + "tag");
                writer.newLine();
            }catch (IOException e){
                System.out.println("Writer cannot write into files");
            }
            while((line = bufferedReader.readLine()) != null){
                JsonNode jsonNode = mapper.readTree(line);
                // Store the current id
                int id = Integer.parseInt(
                        jsonNode.get("_id").toString().replace("\"", ""));
                // Jump to the query with id = 1 and start from there 
                if(id == 1 || lock) {
                    // Open the lock for the rest of the queries
                    lock = true;
                    // Construct the Querry object for later check the correct answer with the id
                    Querry query = new Querry(id,
                            jsonNode.get("text").toString());
                    // Run each query in the queries.json
                    runSingleQuery(query, dv, 100, field, false);
                }
            }
            long end = System.currentTimeMillis();
            long totalTime = end - start;
            System.out.println("Time used to write the results: " + totalTime / 1000 + " seconds");
        } catch (IOException e) {
            throw new RuntimeException("Error to open buffered reader");
        }finally {
            closeAll();
        }
    }

    /**
     * Method to normalize the vector
     * @param vector the vector to be normalized
     * @return the normalized vector
     */
    private Map<String, Double> normalizeVector(Map<String, Double> vector) {
        // Normalized length
        double length = Math.sqrt(vector.values().stream().mapToDouble(v -> Math.pow(v, 2)).sum());

        if (length == 0.0) {
            return vector;
        }
        // Normalize by dividing each term by normalized length
        return vector.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() / length));
    }

    /**
     * Fetch the answer map from the CSV file
     * @return the answer map, key is the id, value is the corpus-id
     */
    public static Map<String, String> fetchAnswer(){
        String csvFile = "src/main/java/test.csv";
        String line;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            Map<String, String> answer = new HashMap<>();
            int i = 0;
            while ((line = br.readLine()) != null) {
                if(i > 0) {
                    // Trim the tab and space
                    String[] columns = line.split("\\s+");
                    // Query-id, Corpus-id
                    answer.put(columns[0], columns[1]);
                }
                // Ignore the first line
                i++;
            }
            return answer;
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    /**
     * Fetch the corpus map
     * @return the corpus map, key is the corpus-id and the value is the text field
     */
    public static Map<String, String> fetchCorpus(){
        try{
            Map<String, String> corpusMap = new HashMap<>();
            BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/java/corpus.jsonl.json"));
            ObjectMapper objectMapper = new ObjectMapper();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                JsonNode jsonNode = objectMapper.readTree(line);
                // Retrieve the fields
                String id = jsonNode.get("_id").asText();
                String text = jsonNode.get("text").asText();
                // Put into the corpus map
                corpusMap.put(id, text);
            }
            return corpusMap;
        }catch (IOException e){
            return Collections.emptyMap();
        }
    }

    public void writeResult(String queryID, String docID, int rank, double score, String runName){
        try{
            writer.write(queryID + "\t" + "Q0\t" + docID + "\t" + rank+"\t" + score + "\t" + runName + "\n");
        }catch (IOException e){
            System.out.println("Something wrong with the writer");
        }
    }

    /**
     * Delete all the indices under the directory
     */
    public static void deleteIndices(){
        Path directory = Paths.get("src/main/java/org/example/indices");

        try (Stream<Path> paths = Files.walk(directory)) {
            // Delete files
            paths.filter(Files::isRegularFile)
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            System.out.println("Deleted file: " + path);
                        } catch (IOException e) {
                            System.err.println("Unable to delete file: " + path);
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close all the resource
     */
    public void closeAll() {
        try {
            this.indexReader.close();
        } catch (IOException e) {
            throw new RuntimeException("Unable to close indexReader", e);
        }

        try {
            this.analyzer.close();
        } catch (Exception e) {
            throw new RuntimeException("Unable to close analyzer", e);
        }

        try {
            this.writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Unable to close writer", e);
        }
    }

    public static void main(String[] args) throws IOException{
        /*
        To run the code:
        1. Delete the previous stored indices (that means you can rerun this main() many times)
        2. Instantiate the indexer and construct the index first: indexer.index();
        3. Build the vector with indexer's analyzer
        4.
           To run all the queries from the queries.json, run the runQueries() directly
         */
        deleteIndices();
        Indexer indexer = new Indexer();
        Map<String, String> list = indexer.index();
        Vector vector = new Vector(indexer.getAnalyzer(), list);
        vector.runQueriesOnField("combined");
    }
}
