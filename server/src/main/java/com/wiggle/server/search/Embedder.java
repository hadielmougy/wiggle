package com.wiggle.server.search;

import java.util.List;

/**
 * Turns text into vectors for semantic search. An implementation names its {@link #model}, which is
 * stored with every vector it makes, and its {@link #dimension}; vectors of different models are
 * never compared. Called off the engine's path, by the search indexer and by a semantic query, so it
 * may be slow and may fail: a failed batch is embedded again later.
 */
public interface Embedder {

    /** A stable id for the model and its settings; a different id means vectors that do not compare. */
    String model();

    int dimension();

    /** One vector of {@link #dimension} values per text, in the same order. */
    List<float[]> embed(List<String> texts);
}
