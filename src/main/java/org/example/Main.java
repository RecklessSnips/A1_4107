package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {
        Indexer indexer = new Indexer();
        indexer.index();
//        search();
//        collect();
//        test1();
//        test2();
    }

    public static void test2() throws IOException{
        Directory directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
        IndexReader indexReader = DirectoryReader.open(directory);
        ClassicSimilarity similarity = new ClassicSimilarity();

        Terms terms = MultiTerms.getTerms(indexReader, "title");
        TermsEnum iterator = terms.iterator();
        PostingsEnum postings = null;
        int counter = 0;
        int numOf0 = 0;
        while (iterator.next() != null) {
            String term = iterator.term().utf8ToString(); // 词项文本
            int docFreq = iterator.docFreq(); // 文档频率
            counter++;
//            System.out.println("Term: " + term + ", Doc Frequency: " + docFreq);

            postings = iterator.postings(postings, PostingsEnum.ALL);
            if (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS){
//                System.out.println(postings.docID());
//                System.out.println(postings.freq());

            }
        }


        Terms terms1 = MultiTerms.getTerms(indexReader, "text");
//        Terms terms = indexReader.getTermVector(docId, field);
        TermsEnum iterator1 = terms1.iterator();
        PostingsEnum postings1 = null;
        int counter1 = 0;
        while (iterator1.next() != null){
            String term = iterator1.term().utf8ToString(); // 词项文本
            int docFreq = iterator1.docFreq(); // 文档频率
            System.out.println("docFreq: " + docFreq);
            counter1++;
//            System.out.println("Term: " + term + ", Doc Frequency: " + docFreq);

            postings1 = iterator1.postings(postings1, PostingsEnum.ALL);
            if (postings1.nextDoc() != DocIdSetIterator.NO_MORE_DOCS){
//                System.out.println(postings1.docID());
//                System.out.println(postings1.freq());
                int tf = postings1.freq();
                float tf1 = similarity.tf(tf);
                int i = indexReader.docFreq(new Term("text", term));
                System.out.println("i: " + i);
                int totalDocs = indexReader.numDocs();
                float idf = similarity.idf(i, totalDocs);
                double tf_idf = tf1 * idf;
                System.out.println(tf_idf);
            }
        }
        System.out.println("Num of 0: " + numOf0);

        Terms terms2 = MultiTerms.getTerms(indexReader, "id");
        TermsEnum iterator2 = terms2.iterator();
        PostingsEnum postings2 = null;
        int counter2 = 0;
        while (iterator2.next() != null){
            String term = iterator2.term().utf8ToString(); // 词项文本
            int docFreq = iterator2.docFreq(); // 文档频率

            postings2 = iterator2.postings(postings2, PostingsEnum.ALL);
            counter2++;
//            System.out.println("Term: " + term + ", Doc Frequency: " + docFreq);
            if (postings2.nextDoc() != DocIdSetIterator.NO_MORE_DOCS){
//                System.out.println(postings2.docID());
//                System.out.println(postings2.freq());

            }
        }

        System.out.println("Title: " + counter);
        System.out.println("Text: " + counter1);
        System.out.println("Id: " + counter2);
        int total = counter + counter1 + counter2;
        System.out.println("Total: " + total);
        System.out.println("# of Docs: " + indexReader.numDocs());
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
//        Analyzer analyzer = new StandardAnalyzer();
        Analyzer analyzer = new EnglishAnalyzer();
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
                TokenStream tokenStream = analyzer.tokenStream("text", querry.getText());
                CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
                tokenStream.reset();
                while (tokenStream.incrementToken()){
                    System.out.println("!!!!!!!" + charTermAttribute);
                }
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
                tokenStream.close();
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        analyzer.close();
        indexReader.close();
    }

    public static void search() throws IOException {
//        Analyzer analyzer = new StandardAnalyzer();
        Analyzer analyzer = new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
//        Analyzer analyzer = new EnglishAnalyzer();
        QueryParser queryParser = new QueryParser("text", analyzer);
        Query query;
        TokenStream stream;
        CharTermAttribute charTermAttribute;
        try {

            query = queryParser.parse("1,000 genomes project enables mapping of genetic sequence variation consisting of rare variants with larger penetrance effects than common variants.");
//            query = queryParser.parse("p16INK4A accumulation is  linked to an abnormal wound response caused by the microinvasive step of advanced Oral Potentially Malignant Lesions (OPMLs).");
//            System.out.println(query);
            stream = analyzer.tokenStream("name", new StringReader("1,000 genomes project enables mapping of genetic sequence variation consisting of rare variants with larger penetrance effects than common variants."));
            charTermAttribute = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()){
                System.out.println(charTermAttribute.toString());
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        Directory directory = FSDirectory.open(Paths.get("src/main/java/org/example/indices"));
        IndexReader indexReader = DirectoryReader.open(directory);

        Terms terms = MultiTerms.getTerms(indexReader, "title");
        TermVectors termVectors = indexReader.termVectors();
        long fieldLength = terms.size();
        float boost = 1.0f;
        float fieldNorm = (float) (1.0 / Math.sqrt(fieldLength)) * boost;
        TermsEnum iterator = terms.iterator();
        while (iterator.next() != null) {
            String term = iterator.term().utf8ToString(); // 词项文本
            int docFreq = iterator.docFreq(); // 文档频率
//            System.out.println("Term: " + term + ", Doc Frequency: " + docFreq);
        }

        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(new ClassicSimilarity());
        TopDocs search = indexSearcher.search(query, 10);
        ScoreDoc[] scoreDocs = search.scoreDocs;
        System.out.println("总数: " + search.totalHits);
        double score = 0.0;
        if(scoreDocs != null){
            for(ScoreDoc scoreDoc: scoreDocs){
                int doc = scoreDoc.doc;
                Explanation explain = indexSearcher.explain(query, doc);
                System.out.println("Score: " + scoreDoc.score);
//                System.out.println(explain.toString());
                Document document = indexSearcher.storedFields().document(doc);
//                System.out.println(document);
//                System.out.println("Details: " + Arrays.toString(explain.getDetails()));
                Explanation[] details = explain.getDetails();
                for(Explanation explanation: details){
                    System.out.println(explanation.getValue());
                    score += explanation.getValue().doubleValue();
                }
                System.out.println("score: " + score);
                score = 0;
                System.out.println("Value: " + explain.getValue());
                System.out.println("Norm: " + fieldNorm);
                System.out.println(score * fieldNorm == explain.getValue().doubleValue());
                System.out.println(explain);
            }
        }else{
            System.out.println("Score Doc is empty");
        }

        stream.close();
        analyzer.close();
        indexReader.close();
    }

    public static List<CharArraySet> getStopWords(){
        BufferedReader reader = null;
        CharArraySet ENGLISH_STOP_WORDS_SET;
        try {
            reader = Files.newBufferedReader(Paths.get("src/main/java/stop_words.txt"));
            String line;
            List<String> stopwords = new LinkedList<>();
            while ((line = reader.readLine()) != null) {
                stopwords.add(line);
            }
            final CharArraySet stopSet = new CharArraySet(stopwords, false);
            ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet);

        }catch (IOException e){
            e.printStackTrace();
            return Collections.singletonList(CharArraySet.EMPTY_SET);
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return Collections.singletonList(ENGLISH_STOP_WORDS_SET);
    }
}