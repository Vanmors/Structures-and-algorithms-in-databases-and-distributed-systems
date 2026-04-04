package com.vanmors.lsh;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;


public class NearDuplicateDemo {
    public static void main(final String[] args) {
        // Параметры
        final int shingleSize = 3;               // k-шинглы (по словам)
        final int numHashFunctions = 100;        // количество MinHash
        final double thresholdApprox = 0.75;     // желаемый порог схожести

        final LSH lsh = new LSH(numHashFunctions, thresholdApprox);

        // Примеры текстов
        final Map<String, String> documents = new LinkedHashMap<>();
        documents.put("doc1", "кошка ловит мышь в доме каждый день");
        documents.put("doc2", "кошка ловит мышку дома каждый день");     // почти дубликат
        documents.put("doc3", "собака бегает по парку с мячом");
        documents.put("doc4", "кошка спит на диване весь день");
        documents.put("doc5", "кошка ловит мышь в доме каждый вечер");   // ещё один похожий

        // Вставляем все документы
        final Map<String, MinHashSignature> signatures = new HashMap<>();
        for (final Map.Entry<String, String> entry : documents.entrySet()) {
            final String id = entry.getKey();
            final String text = entry.getValue();

            final Set<String> shingles = LSH.shingleGenerator(text, shingleSize);
            final MinHashSignature sig = LSH.computeMinHash(shingles, numHashFunctions);

            signatures.put(id, sig);
            lsh.insert(id, sig);
        }

        // Ищем дубликаты для каждого документа
        System.out.println("Найденные почти-дубликаты:");
        for (final String queryId : documents.keySet()) {
            final MinHashSignature querySig = signatures.get(queryId);
            final Set<String> candidates = lsh.query(queryId, querySig);

            if (!candidates.isEmpty()) {
                System.out.printf("%s → %s%n", queryId, candidates);
                // можно посчитать точный Jaccard для кандидатов
                for (final String cand : candidates) {
                    final double exactJ = querySig.jaccard(signatures.get(cand));
                    System.out.printf("   %s → Jaccard = %.3f%n", cand, exactJ);
                }
            }
        }
    }
}
