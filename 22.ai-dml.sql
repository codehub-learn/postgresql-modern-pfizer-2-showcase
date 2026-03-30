-- =====================================================
-- Basic Semantic Search with Cosine Distance
-- Demonstrates: Finding the closest movies to a search phrase embedding
-- NOTE: <=> is cosine distance operator
-- =====================================================
WITH query_embedding AS (SELECT omdb.get_embedding('May the force be with you') AS emb)
SELECT id,
       name,
       description,
       movie_embedding <=> query_embedding.emb AS cosine_distance
FROM omdb.movies,
     query_embedding
WHERE movie_embedding IS NOT NULL
ORDER BY movie_embedding <=> query_embedding.emb
LIMIT 10;


-- =====================================================
-- Semantic Search Returning Similarity Score
-- Demonstrates: Converting cosine distance into a more readable similarity metric
-- =====================================================
SELECT id,
       name,
       ROUND((1 - (movie_embedding <=> omdb.get_embedding('May the force be with you')))::numeric, 4) AS similarity_score
FROM omdb.movies
WHERE movie_embedding IS NOT NULL
ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
LIMIT 10;


-- =====================================================
-- Semantic Search with Release Date Filter
-- Demonstrates: Combining vector similarity with structured filtering
-- =====================================================
SELECT id,
       name,
       release_date,
       vote_average,
       movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
FROM omdb.movies
WHERE movie_embedding IS NOT NULL
  AND release_date >= DATE '2010-01-01'
ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
LIMIT 10;


-- =====================================================
-- Semantic Search with Rating Threshold
-- Demonstrates: Retrieving semantically relevant but also highly rated movies
-- =====================================================
SELECT id,
       name,
       vote_average,
       votes_count,
       movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
FROM omdb.movies
WHERE movie_embedding IS NOT NULL
  AND vote_average >= 7.5
  AND votes_count >= 1000
ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
LIMIT 10;


-- =====================================================
-- Semantic Search with Revenue Filter
-- Demonstrates: Mixing AI search with business-oriented structured criteria
-- =====================================================
SELECT id,
       name,
       revenue,
       budget,
       movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
FROM omdb.movies
WHERE movie_embedding IS NOT NULL
  AND revenue > 100000000
ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
LIMIT 10;


-- =====================================================
-- Top-K Nearest Neighbor Search
-- Demonstrates: Canonical retrieval pattern for recommendation/search systems
-- =====================================================
SELECT id,
       name,
       movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
FROM omdb.movies
WHERE movie_embedding IS NOT NULL
ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
LIMIT 5;


-- =====================================================
-- Semantic Search Using a CTE for Reusability
-- Demonstrates: Computing the query embedding once and reusing it
-- =====================================================
WITH query_embedding AS (SELECT omdb.get_embedding('A pirate captain who sails the seven seas in search of treasure') AS emb)
SELECT m.id,
       m.name,
       m.vote_average,
       m.movie_embedding <=> q.emb AS cosine_distance
FROM omdb.movies m
         CROSS JOIN query_embedding q
WHERE m.movie_embedding IS NOT NULL
ORDER BY m.movie_embedding <=> q.emb
LIMIT 10;


-- =====================================================
-- Full-Text Search Only
-- Demonstrates: Lexical keyword search without vectors
-- =====================================================
SELECT id,
       name,
       ts_rank(search_vector,
               websearch_to_tsquery('english', '"A pirate captain who sails the seven seas in search of treasure"')) AS text_rank
FROM omdb.movies
WHERE search_vector @@ websearch_to_tsquery('english', '"A pirate captain who sails the seven seas in search of treasure"')
ORDER BY text_rank DESC
LIMIT 10;


-- =====================================================
-- Hybrid Search with Simple Combined Ranking
-- Demonstrates: Combining semantic similarity and full-text relevance
-- =====================================================
WITH query_data AS (SELECT omdb.get_embedding('May the force be with you')                AS emb,
                           websearch_to_tsquery('english', '"May the force be with you"') AS tsq)
SELECT m.id,
       m.name,
       ROUND((1 - (m.movie_embedding <=> q.emb))::numeric, 4)                                                 AS semantic_score,
       ROUND(ts_rank(m.search_vector, q.tsq)::numeric, 4)                                                     AS lexical_score,
       ROUND(((1 - (m.movie_embedding <=> q.emb)) * 0.7 + ts_rank(m.search_vector, q.tsq) * 0.3)::numeric, 4) AS hybrid_score
FROM omdb.movies m
         CROSS JOIN query_data q
WHERE m.movie_embedding IS NOT NULL
  AND m.search_vector @@ q.tsq
ORDER BY hybrid_score DESC
LIMIT 10;


-- =====================================================
-- Hybrid Search with Broader Candidate Set
-- Demonstrates: Allowing either semantic closeness or text match before final ranking
-- =====================================================
WITH query_data AS (SELECT omdb.get_embedding('May the force be with you')                AS emb,
                           websearch_to_tsquery('english', '"May the force be with you"') AS tsq)
SELECT m.id,
       m.name,
       ROUND((1 - (m.movie_embedding <=> q.emb))::numeric, 4)                                                              AS semantic_score,
       ROUND(COALESCE(ts_rank(m.search_vector, q.tsq), 0)::numeric, 4)                                                     AS lexical_score,
       ROUND(((1 - (m.movie_embedding <=> q.emb)) * 0.8 + COALESCE(ts_rank(m.search_vector, q.tsq), 0) * 0.2)::numeric, 4) AS hybrid_score
FROM omdb.movies m
         CROSS JOIN query_data q
WHERE m.movie_embedding IS NOT NULL
  AND (
    m.search_vector @@ q.tsq
        OR (m.movie_embedding <=> q.emb) < 0.45
    )
ORDER BY hybrid_score DESC
LIMIT 15;


-- =====================================================
-- Hybrid Search with Business Ranking Signals
-- Demonstrates: Mixing AI relevance with rating and popularity signals
-- =====================================================
WITH query_data AS (SELECT omdb.get_embedding('May the force be with you') AS emb)
SELECT m.id,
       m.name,
       m.vote_average,
       m.votes_count,
       ROUND((1 - (m.movie_embedding <=> q.emb))::numeric, 4) AS semantic_score,
       ROUND((
                 (1 - (m.movie_embedding <=> q.emb)) * 0.6 +
                 COALESCE(m.vote_average / 10.0, 0) * 0.25 +
                 LEAST(COALESCE(m.votes_count, 0), 100000)::numeric / 100000 * 0.15
                 )::numeric, 4)                               AS final_score
FROM omdb.movies m
         CROSS JOIN query_data q
WHERE m.movie_embedding IS NOT NULL
ORDER BY final_score DESC
LIMIT 10;


-- =====================================================
-- Semantic Search with Aggregated Analytics
-- Demonstrates: Summarizing the top semantically similar result set
-- =====================================================
WITH top_matches AS (SELECT id,
                            name,
                            release_date,
                            runtime,
                            budget,
                            revenue,
                            vote_average,
                            votes_count,
                            movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
                     FROM omdb.movies
                     WHERE movie_embedding IS NOT NULL
                     ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
                     LIMIT 20)
SELECT COUNT(*)          AS matched_movies,
       AVG(runtime)      AS avg_runtime,
       AVG(vote_average) AS avg_vote_average,
       AVG(budget)       AS avg_budget,
       AVG(revenue)      AS avg_revenue,
       MIN(release_date) AS oldest_release_date,
       MAX(release_date) AS newest_release_date
FROM top_matches;


-- =====================================================
-- Group Semantic Matches by Release Decade
-- Demonstrates: Analytical reporting on retrieved semantic neighborhoods
-- =====================================================
WITH top_matches AS (SELECT id,
                            name,
                            release_date,
                            movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
                     FROM omdb.movies
                     WHERE movie_embedding IS NOT NULL
                       AND release_date IS NOT NULL
                     ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
                     LIMIT 50)
SELECT (EXTRACT(YEAR FROM release_date)::int / 10) * 10 AS release_decade,
       COUNT(*)                                         AS movies_in_decade
FROM top_matches
GROUP BY release_decade
ORDER BY release_decade;


-- =====================================================
-- Find Movies Similar to Another Movie
-- Demonstrates: Movie-to-movie recommendation using an existing movie embedding
-- =====================================================
SELECT m2.id,
       m2.name,
       m2.movie_embedding <=> m1.movie_embedding AS cosine_distance
FROM omdb.movies m1
         JOIN omdb.movies m2
              ON m1.id <> m2.id
WHERE m1.name = 'Inception'
  AND m1.movie_embedding IS NOT NULL
  AND m2.movie_embedding IS NOT NULL
ORDER BY m2.movie_embedding <=> m1.movie_embedding
LIMIT 10;


-- =====================================================
-- Find Movies Similar to Another Movie by ID
-- Demonstrates: More stable recommendation query using primary key lookup
-- =====================================================
SELECT m2.id,
       m2.name,
       ROUND((1 - (m2.movie_embedding <=> m1.movie_embedding))::numeric, 4) AS similarity_score
FROM omdb.movies m1
         JOIN omdb.movies m2
              ON m1.id <> m2.id
WHERE m1.id = 100
  AND m1.movie_embedding IS NOT NULL
  AND m2.movie_embedding IS NOT NULL
ORDER BY m2.movie_embedding <=> m1.movie_embedding
LIMIT 10;


-- =====================================================
-- Phrase-to-Phrase Similarity Search
-- Demonstrates: Finding semantically related user queries in the dictionary
-- =====================================================
SELECT d2.phrase,
       d2.phrase_embedding <=> d1.phrase_embedding AS cosine_distance
FROM omdb.phrases_dictionary d1
         JOIN omdb.phrases_dictionary d2
              ON LOWER(d1.phrase) <> LOWER(d2.phrase)
WHERE LOWER(d1.phrase) = LOWER('May the force be with you')
  AND d1.phrase_embedding IS NOT NULL
  AND d2.phrase_embedding IS NOT NULL
ORDER BY d2.phrase_embedding <=> d1.phrase_embedding
LIMIT 10;


-- =====================================================
-- Use Dictionary Phrases to Retrieve Movies
-- Demonstrates: Running semantic search for multiple stored phrases
-- =====================================================
SELECT d.phrase,
       m.id,
       m.name,
       m.movie_embedding <=> d.phrase_embedding AS cosine_distance
FROM omdb.phrases_dictionary d
         CROSS JOIN LATERAL (
    SELECT id,
           name,
           movie_embedding
    FROM omdb.movies
    WHERE movie_embedding IS NOT NULL
    ORDER BY movie_embedding <=> d.phrase_embedding
    LIMIT 3
    ) m
WHERE d.phrase_embedding IS NOT NULL
ORDER BY d.phrase, cosine_distance;


-- =====================================================
-- Semantic Search with Window Function Ranking
-- Demonstrates: Ranking neighbors and keeping ordering metadata
-- =====================================================
WITH query_embedding AS (SELECT omdb.get_embedding('May the force be with you') AS emb),
     ranked_matches AS (SELECT m.id,
                               m.name,
                               m.release_date,
                               m.movie_embedding <=> q.emb                              AS cosine_distance,
                               ROW_NUMBER() OVER (ORDER BY m.movie_embedding <=> q.emb) AS semantic_rank
                        FROM omdb.movies m
                                 CROSS JOIN query_embedding q
                        WHERE m.movie_embedding IS NOT NULL)
SELECT semantic_rank,
       id,
       name,
       release_date,
       cosine_distance
FROM ranked_matches
WHERE semantic_rank <= 10
ORDER BY semantic_rank;


-- =====================================================
-- Semantic Search with Threshold
-- Demonstrates: Returning only sufficiently similar items
-- =====================================================
SELECT id,
       name,
       movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
FROM omdb.movies
WHERE movie_embedding IS NOT NULL
  AND (movie_embedding <=> omdb.get_embedding('May the force be with you')) < 0.35
ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
LIMIT 20;


-- =====================================================
-- Inspect Query Execution Plan for Semantic Search
-- Demonstrates: Verifying whether PostgreSQL uses the pgvector index
-- =====================================================
EXPLAIN ANALYZE
SELECT id,
       name,
       movie_embedding <=> omdb.get_embedding('May the force be with you') AS cosine_distance
FROM omdb.movies
WHERE movie_embedding IS NOT NULL
ORDER BY movie_embedding <=> omdb.get_embedding('May the force be with you')
LIMIT 10;


-- =====================================================
-- Inspect Query Execution Plan for Hybrid Search
-- Demonstrates: Understanding execution behavior for combined AI + text search
-- =====================================================
EXPLAIN ANALYZE
WITH query_data AS (SELECT omdb.get_embedding('May the force be with you')                AS emb,
                           websearch_to_tsquery('english', '"May the force be with you"') AS tsq)
SELECT m.id,
       m.name,
       ((1 - (m.movie_embedding <=> q.emb)) * 0.7 + ts_rank(m.search_vector, q.tsq) * 0.3) AS hybrid_score
FROM omdb.movies m
         CROSS JOIN query_data q
WHERE m.movie_embedding IS NOT NULL
  AND m.search_vector @@ q.tsq
ORDER BY hybrid_score DESC
LIMIT 10;
