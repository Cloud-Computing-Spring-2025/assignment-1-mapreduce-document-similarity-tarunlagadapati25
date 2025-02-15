package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\\s+", 2);

        if (parts.length < 2)
            return; // Ignore malformed lines

        String documentId = parts[0];
        String content = parts[1];

        HashSet<String> uniqueWords = new HashSet<>();
        StringTokenizer tokenizer = new StringTokenizer(content);

        while (tokenizer.hasMoreTokens()) {
            uniqueWords.add(tokenizer.nextToken().toLowerCase());
        }

        // Emit (word, documentID) for proper grouping in the reducer
        for (String word : uniqueWords) {
            context.write(new Text(word), new Text(documentId));
        }
    }
}