-- =====================================================
-- INSTALL PG_Vector extension to PostgreSQL 18 Bookworm Edition
-- apt-get install -y --no-install-recommends postgresql-18-pgvector
--
--
-- Concept 1 — Embeddings are NOT text search
-- - SQL full-text search → flexible (LIKE, tsquery)
-- - embeddings → require vector representation
--
-- Concept 2 — Embedding generation is external
-- - PostgreSQL does NOT create embeddings
--
-- It only:
-- - stores them
-- - indexes them
-- - searches them
--
-- Concept 3 — Deterministic vs dynamic systems
-- Your function = deterministic system: input must exist → otherwise fail
-- Real AI system = dynamic: any input → generate embedding → always works
--
-- Why you added the EXCEPTION (and why it's actually good)
--
-- This enforces:
-- - correctness
-- - visibility (no silent failures)
-- - predictable behavior for demos
-- =====================================================

-- =====================================================
-- Enable pgvector Extension
-- Demonstrates: Activating vector support in PostgreSQL
-- =====================================================
CREATE EXTENSION IF NOT EXISTS vector;

-- =====================================================
-- Recreate Movies Table
-- Demonstrates: Storing movies together with embeddings
-- =====================================================
DROP TABLE IF EXISTS omdb.movies;
CREATE TABLE IF NOT EXISTS omdb.movies
(
    id              BIGINT PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT NOT NULL,
    movie_embedding VECTOR(1024),
    release_date    DATE,
    runtime         INT,
    budget          NUMERIC,
    revenue         NUMERIC,
    vote_average    NUMERIC,
    votes_count     BIGINT
);

-- =================================================
-- Add Generated Full-Text Search Column
-- Demonstrates: Preparing hybrid search with full-text + vectors
-- =====================================================
ALTER TABLE omdb.movies
    DROP COLUMN IF EXISTS search_vector;

ALTER TABLE omdb.movies
    ADD COLUMN search_vector tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'B')
        ) STORED;

-- =====================================================
-- Recreate Phrases Dictionary Table
-- Demonstrates: Dictionary of phrases mapped to embeddings
-- =====================================================
DROP TABLE IF EXISTS omdb.phrases_dictionary;
CREATE TABLE omdb.phrases_dictionary
(
    phrase           TEXT NOT NULL,
    phrase_embedding VECTOR(1024)
);

-- =====================================================
-- Create Embedding Lookup Function
-- Demonstrates: Retrieving a precomputed embedding by phrase
-- =====================================================
CREATE OR REPLACE FUNCTION omdb.get_embedding(input_phrase TEXT)
    RETURNS VECTOR(1024) AS
$$
DECLARE
    embedding VECTOR(1024);
BEGIN
    SELECT phrase_embedding
    INTO embedding
    FROM omdb.phrases_dictionary
    WHERE LOWER(phrase) = LOWER(input_phrase);

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'The search phrase does not exist in the dictionary table.';
    END IF;

    RETURN embedding;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Create GIN Index for Full-Text Search
-- Demonstrates: Production-grade indexing for lexical search
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_movies_search_vector
    ON omdb.movies
        USING GIN (search_vector);

-- =====================================================
-- Create HNSW Index for Cosine Similarity
-- Demonstrates: Fast approximate nearest-neighbor search for embeddings
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_movies_embedding_hnsw_cosine
    ON omdb.movies
        USING hnsw (movie_embedding vector_cosine_ops);


-- =====================================================
-- Create HNSW Index for Inner Product Similarity
-- Demonstrates: Alternative ANN index for embeddings when inner product is preferred
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_movies_embedding_hnsw_ip
    ON omdb.movies
        USING hnsw (movie_embedding vector_ip_ops);


-- =====================================================
-- Create IVFFlat Index for Cosine Similarity
-- Demonstrates: Another ANN indexing option requiring training/probing
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_movies_embedding_ivfflat_cosine
    ON omdb.movies
        USING ivfflat (movie_embedding vector_cosine_ops)
    WITH (lists = 100);
