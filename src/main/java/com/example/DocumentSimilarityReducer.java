package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {

    private final Map<String, Set<String>> documentWordMap = new HashMap<>();

    @Override
    public void reduce(Text word, Iterable<Text> documentIds, Context context)
            throws IOException, InterruptedException {
        List<String> docList = new ArrayList<>();

        for (Text doc : documentIds) {
            String docName = doc.toString();
            docList.add(docName);

            // Store words for each document
            documentWordMap.putIfAbsent(docName, new HashSet<>());
            documentWordMap.get(docName).add(word.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<String> docList = new ArrayList<>(documentWordMap.keySet());

        // Compare each document pair and compute Jaccard Similarity
        for (int i = 0; i < docList.size(); i++) {
            for (int j = i + 1; j < docList.size(); j++) {
                String doc1 = docList.get(i);
                String doc2 = docList.get(j);

                double similarity = computeJaccardSimilarity(doc1, doc2);

                if (similarity > 0) {
                    String outputKey = "<" + doc1 + ", " + doc2 + ">";
                    String outputValue = " -> " + String.format("%.2f", similarity * 100) + "%";
                    context.write(new Text(outputKey), new Text(outputValue));
                }
            }
        }
    }

    private double computeJaccardSimilarity(String doc1, String doc2) {
        Set<String> wordsDoc1 = documentWordMap.getOrDefault(doc1, new HashSet<>());
        Set<String> wordsDoc2 = documentWordMap.getOrDefault(doc2, new HashSet<>());

        Set<String> intersection = new HashSet<>(wordsDoc1);
        intersection.retainAll(wordsDoc2); // Compute |A ∩ B|

        Set<String> union = new HashSet<>(wordsDoc1);
        union.addAll(wordsDoc2); // Compute |A ∪ B|

        if (union.isEmpty())
            return 0; // Avoid division by zero

        return (double) intersection.size() / union.size();
    }
}