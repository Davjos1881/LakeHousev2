-- TRINO: tablas Silver y replica de Gold desde Silver
-- Ejecutar con:  Get-Content -Raw sql\trino_silver_y_replica_gold.sql | docker exec -i trino_fhbd_2026 trino

-- 1) Tablas Silver disponibles en Trino
SHOW TABLES FROM iceberg.silver;
SELECT count(*) AS filas_posts_hist FROM iceberg.silver.posts_hist;
SELECT count(*) AS filas_users_hist FROM iceberg.silver.users_hist;

-- 2) Replica de gold.post_counts_by_user calculada SOLO desde Silver (devuelve todos los registros).
-- Mismos parametros que gold_agg.ipynb: TIER_ALTO = 10, TIER_MEDIO = 3, anios 2020 y 2021.
-- En Trino BIGINT / BIGINT es division entera, por eso los CAST a double.
WITH elegibles AS (
  SELECT post_id, post_type_id, owner_user_id, creation_date, score
  FROM iceberg.silver.posts_hist
  WHERE post_type_id IN (1, 2) AND owner_user_id IS NOT NULL
),
aceptadas AS (
  SELECT DISTINCT accepted_answer_id AS post_id
  FROM iceberg.silver.posts_hist
  WHERE post_type_id = 1 AND accepted_answer_id IS NOT NULL
),
por_usuario AS (
  SELECT
    e.owner_user_id AS user_id,
    count_if(e.post_type_id = 1) AS questions,
    count_if(e.post_type_id = 2) AS answers,
    count(*) AS total_posts,
    count_if(year(e.creation_date AT TIME ZONE 'UTC') = 2020) AS posts_2020,
    count_if(year(e.creation_date AT TIME ZONE 'UTC') = 2021) AS posts_2021,
    sum(e.score) AS total_score,
    count_if(e.post_type_id = 2 AND a.post_id IS NOT NULL) AS accepted_answers,
    min(e.creation_date) AS first_post_date,
    max(e.creation_date) AS last_post_date
  FROM elegibles e
  LEFT JOIN aceptadas a ON e.post_id = a.post_id
  GROUP BY e.owner_user_id
)
SELECT
  p.user_id, u.display_name, u.location, u.reputation,
  u.creation_date AS account_created, u.last_access_date,
  p.questions, p.answers, p.total_posts, p.posts_2020, p.posts_2021,
  p.total_score,
  round(CAST(p.total_score AS double) / p.total_posts, 2) AS avg_score_per_post,
  p.accepted_answers,
  CASE WHEN p.answers > 0 THEN round(CAST(p.accepted_answers AS double) / p.answers, 4) END AS acceptance_rate,
  p.first_post_date, p.last_post_date,
  CASE WHEN p.questions > 0 THEN round(CAST(p.answers AS double) / p.questions, 2) END AS answers_per_question,
  CASE WHEN p.total_posts >= 10 THEN 'alto'
       WHEN p.total_posts >= 3 THEN 'medio'
       ELSE 'bajo' END AS activity_tier,
  CASE WHEN p.posts_2021 > p.posts_2020 THEN 'creciente'
       WHEN p.posts_2021 < p.posts_2020 THEN 'decreciente'
       ELSE 'estable' END AS activity_trend
FROM por_usuario p
LEFT JOIN iceberg.silver.users_hist u ON p.user_id = u.user_id
ORDER BY p.total_posts DESC;