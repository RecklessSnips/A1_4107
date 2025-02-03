package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Vector {

    private final ClassicSimilarity similarity = new ClassicSimilarity();

    private IndexReader indexReader;

    private Analyzer analyzer;

    private Map<Integer, Map<String, Double>> documentVector;
    private Map<String, Double> queryVector;

    private final int TOTALDOCS;

    public Vector(Analyzer analyzer){
        initialize();
        documentVector = new HashMap<>();
        queryVector = new HashMap<>();
        TOTALDOCS = indexReader.numDocs();
        this.analyzer = analyzer;
    }

    public void initialize(){
        try {
            Directory directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
            indexReader = DirectoryReader.open(directory);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public Map<Integer, Map<String, Double>> buildDocumentVector(String field){
        for (int i = 0; i < TOTALDOCS; i++) {
            Map<String, Double> termsWeight = buildDocumentVectorHelper(i, field);
            documentVector.put(i, termsWeight);
        }
        return documentVector;
    }

    private Map<String, Double> buildDocumentVectorHelper(int docID, String field){
        try {
            TermVectors termVectors = indexReader.termVectors();
            Terms terms = termVectors.get(docID, field);

            // TODO: Remove 2 lines
            long totalTerms = terms.getSumTotalTermFreq();
            double lengthNorm = 1.0 / Math.sqrt(Math.sqrt(totalTerms));

            Map<String, Double> tfIdfMap = new HashMap<>();
            // text, title
//            for(String names: strings){
//                Terms terms = strings.terms(names);
//                Terms terms = strings.terms("text");
                TermsEnum iterator = terms.iterator();
                // 遍历每一个term
                while (iterator.next() != null){
                    String s = iterator.term().utf8ToString();
//                    System.out.println("??" + s);
                    // 将每一个term的 tf 找到
                    long frequency = iterator.totalTermFreq();
                    double tf = similarity.tf(frequency);

                    int df = indexReader.docFreq(new Term(field, s));
                    double idf = similarity.idf(df, TOTALDOCS);
//                    double idf = Math.log10((double) TOTALDOCS / (df + 1e-12));

//                    double tf_idf = tf * idf;
                    double tf_idf = frequency * idf;
//                    System.out.println("tf: " + s + " is: " + tf);
//                    System.out.println("idf: " + s + " is: " + idf);
//                    System.out.println("tf_idf for " + s + " is: " + tf_idf + " in the document: " + docID);
                    // Create Document vector, later will put into the document vector map
                    tfIdfMap.put(s, tf_idf);
                }
//            }

            Map<String, Double> stringDoubleMap = normalizeVector(tfIdfMap);
            tfIdfMap.clear();
            tfIdfMap.putAll(stringDoubleMap);
            return tfIdfMap;
        }catch (IOException e){
            return Collections.emptyMap();
        }
    }

    private int countOccurrence(String query, String target){
        int counter = 0;
        int index = query.indexOf(target);
        while (index != -1){
            counter++;
            index = query.indexOf(target, index + target.length());
        }
        return counter;
    }

    public Map<String, Double> buildQueryVector(String query, String field){
        try {
            List<String> tokens = tokenizeQuery(query);
            System.out.println(query);

            Map<String, Long> termFreqMap = tokens.stream()
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

            // TODO: Remove 4 lines
            double normalize = termFreqMap.values().stream()
                    .mapToDouble(freq -> Math.pow(freq, 2))
                    .sum();
            double norm = Math.sqrt(normalize);


            for(String token: tokens){
                long frequency = tokens.stream().filter(t -> t.equals(token)).count();
//                System.out.println(countOccurrence(query, token));
//                System.out.println(query.split(" ").length);
//                double frequency = (double) countOccurrence(query, token) / query.split(" ").length;
//                double tf = similarity.tf(frequency);
                int df = indexReader.docFreq(new Term(field, token));
                double idf = similarity.idf(df, TOTALDOCS);
//                double idf = Math.log10((double) TOTALDOCS / (df + 1e-12));
//                double tf_idf = tf * idf;
                double tf_idf = (frequency) * idf;
                queryVector.put(token, tf_idf);
            }
            Map<String, Double> stringDoubleMap = normalizeVector(queryVector);
            Map<String, Double> result = new HashMap<>(stringDoubleMap);
//            Map<String, Double> result = new HashMap<>(queryVector);
            queryVector.clear();
            return result;
        } catch (IOException e) {
            return Collections.emptyMap();
        }
    }

    private List<String> tokenizeQuery(String query){
        try(TokenStream tokenStream
                    = analyzer.tokenStream(null, new StringReader(query))){
            List<String> tokens = new LinkedList<>();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()){
                tokens.add(charTermAttribute.toString());
            }
            tokenStream.end();
            return tokens;
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    public LinkedHashMap<Integer, Double> computeCosineSimilarity(Map<Integer, Map<String, Double>> documentVector,
                                        Map<String, Double> queryVector){
//        List<Double> similarities = new LinkedList<>();
        // DocID, 和当前qv的similarity
        Map<Integer, Double> similarities = new HashMap<>();
        for(Map.Entry<Integer, Map<String, Double>> mapEntry: documentVector.entrySet()){
            Map<String, Double> value = mapEntry.getValue();
            double cosineSimilarity = cosineSimilarityHelper(value, queryVector);
            if(cosineSimilarity > 0.0) {
//                similarities.add(cosineSimilarity);
                similarities.put(mapEntry.getKey(), cosineSimilarity);
            }
        }
//        similarities.sort( (a, b) -> Double.compare(b, a));
        LinkedHashMap<Integer, Double> sortedSimilarities = new LinkedHashMap<>();
        similarities.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .forEachOrdered(entry -> sortedSimilarities.put(entry.getKey(), entry.getValue()));
        return sortedSimilarities;
    }

    public double cosineSimilarityHelper(Map<String, Double> dv, Map<String, Double> qv){
        double dotProduct = 0.0;
        double euclideanLength_query = 0.0;
        double euclideanLength_document = 0.0;

        for(Map.Entry<String, Double> entry : qv.entrySet()){
            String term = entry.getKey();
            double query_ifidf = entry.getValue();
            double document_tfidf = dv.getOrDefault(term, 0.0);
            dotProduct += query_ifidf * document_tfidf;
            euclideanLength_query += Math.pow(query_ifidf, 2);
        }

        for(Map.Entry<String, Double> entry : dv.entrySet()){
            double normalized_tfidf = entry.getValue();
            euclideanLength_document += Math.pow(normalized_tfidf, 2);
        }

        double euclideanLength = Math.sqrt(euclideanLength_query)
                                * Math.sqrt(euclideanLength_document);
//        if (dotProduct != 0) {
//            System.out.println("Dot: " + dotProduct);
//            System.out.println("Query Euc: " + euclideanLength_query);
//            System.out.println("Doc Euc: " + euclideanLength_document);
//            System.out.println("Length: " + euclideanLength);
//            System.out.println();
//        }
        return dotProduct / euclideanLength;
    }

    public void runSingleQuery(Querry query, Map<Integer, Map<String, Double>> dv,
                               int limit, boolean ifClose){
        Map<String, Double> qv = buildQueryVector(query.getText(), "text");

        Map<String, String> answerSet = fetchAnswer();
        String answerID = answerSet.get(String.valueOf(query.getId()));
        System.out.println("Answer ID: " + answerID);
        Map<String, String> answerMap = fetchCorpus();
        String answer = answerMap.get(answerID);
        System.out.println("Answer: " + answer);

        System.out.println("query_id\t" +
                            "Q0\t" +
                            "doc_id\t" +
                            "rank\t" +
                            "score\t" +
                            "tag\t");

        int counter = 1;
        int rank = 0;
        // Sorted similarities
        LinkedHashMap<Integer, Double> integerDoubleLinkedHashMap = computeCosineSimilarity(dv, qv);
        for (Map.Entry<Integer, Double> entry : integerDoubleLinkedHashMap.entrySet()){
            if(counter > limit) break;
            rank++;

            int docId = entry.getKey();
            double similarity = entry.getValue();
            try {
                Document doc = indexReader.storedFields().document(docId);
                String text = doc.get("text");
                if (text.equals(answer)){
//                    System.out.println("Found!!!");
//                    System.out.println(text);
                    break;
                }
                // TODO: Recover this line
                System.out.println(similarity + ": " + doc.get("text"));
                counter++;
            } catch (IOException e) {
                throw new RuntimeException("There's a low-level IO error");
            }
        }
        if (rank >= limit){
            System.out.println("Not found within the limit");
        }else{
            rank --;
            System.out.println("Found in rank: " + rank);
        }
        if(ifClose){
            closeAll();
        }
    }

    public void runQueries(){
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/java/queries.jsonl.json"))){

            ObjectMapper mapper = new ObjectMapper();
            String line;
            boolean flag = false;
            Map<Integer, Map<String, Double>> dv = buildDocumentVector("text");
            while((line = bufferedReader.readLine()) != null){
                JsonNode jsonNode = mapper.readTree(line);
                int id = Integer.parseInt(
                        jsonNode.get("_id").toString().replace("\"", ""));
                if(id == 1 || flag) {
                    flag = true;
                    Querry query = new Querry(id,
                            jsonNode.get("text").toString());
                    runSingleQuery(query, dv, 5, false);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error to open buffered reader");
        }
    }

    public static void main(String[] args) throws IOException{
        Indexer indexer = new Indexer();
        Vector vector = new Vector(indexer.getAnalyzer());

        Map<Integer, Map<String, Double>> dv = vector.buildDocumentVector("text");
//        String query = "0-dimensional biomaterials show inductive properties.";
//        String query = "1,000 genomes project enables mapping of genetic sequence variation consisting of rare variants with larger penetrance effects than common variants.";
//        String query = "1/2000 in UK have abnormal PrP positivity.";
        String query = "5% of perinatal mortality is due to low birth weight.";

//        vector.runSingleQuery(new Querry(0, query), dv, 20, true);
        vector.runQueries();
    }

    private Map<String, Double> normalizeVector(Map<String, Double> vector) {
        // Normalized length
        double length = Math.sqrt(vector.values().stream().mapToDouble(v -> Math.pow(v, 2)).sum());

        if (length == 0.0) {
            return vector;
        }

        return vector.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() / length));
    }

    public static Map<String, String> fetchAnswer(){
        String csvFile = "src/main/java/test.csv";  // 替换为你的 CSV 文件路径
        String line;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // 读取每一行
            Map<String, String> answer = new HashMap<>();
            int i = 0;
            while ((line = br.readLine()) != null) {
                if(i > 0) {
                    String[] columns = line.split("\\s+");  // 匹配一个或多个空白符（空格、TAB 等）

                    // Query-id, Corpus-id
                    answer.put(columns[0], columns[1]);
                }
                i++;
            }
            return answer;
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyMap();
        }
    }

    public static Map<String, String> fetchCorpus(){
        try{
            Map<String, String> answerMap = new HashMap<>();
            BufferedReader bufferedReader = Files.newBufferedReader(Paths.get("src/main/java/corpus.jsonl.json"));
            ObjectMapper objectMapper = new ObjectMapper();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                JsonNode jsonNode = objectMapper.readTree(line);
                String id = jsonNode.get("_id").asText();
                String text = jsonNode.get("text").asText();
                answerMap.put(id, text);
            }
            return answerMap;
        }catch (IOException e){
            e.printStackTrace();
            return Collections.emptyMap();
        }
    }

    public void closeAll(){
        try{
            this.indexReader.close();
        }catch (IOException e){
            throw new RuntimeException("Unable to close the reader");
        }
    }
}
