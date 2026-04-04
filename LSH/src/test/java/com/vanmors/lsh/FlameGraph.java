package com.vanmors.lsh;

import org.instancio.Instancio;
import org.instancio.Select;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class FlameGraph {
    public static void main(final String[] args) {
        final int documentCount = 10000;
        final LSH lsh = new LSH(120, 0.75);
        final List<MinHashSignature> signatures =  new ArrayList<>(documentCount);
        final List<String> docIds = new ArrayList<>(documentCount);
        // Генерируем документы
        final List<String> texts = Instancio.ofList(String.class)
                .size(documentCount)
                .generate(Select.allStrings(), gen -> gen.string().length(20, 80))
                .create();


        for (int i = 0; i < documentCount; i++) {
            final Set<String> shingles = LSH.shingleGenerator(texts.get(i), 3);
            final MinHashSignature sig = LSH.computeMinHash(shingles, 120);
            signatures.add(sig);
            docIds.add("doc" + i);
        }

        // Вставляем все документы
        for (int i = 0; i < documentCount; i++) {
            lsh.insert(docIds.get(i), signatures.get(i));
        }
    }
}
