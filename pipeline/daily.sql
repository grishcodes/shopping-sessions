CREATE MACRO calendar(s) AS try_strptime(s, '%Y-%m-%dT%H:%M:%S');
CREATE MACRO milliseconds(s) AS CASE
    WHEN regexp_full_match(s, '[0-9]+') THEN try_cast(s AS BIGINT)
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]Z')
        THEN epoch_ms(calendar(left(s, 19)))
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9][+-]([01][0-9]|2[0-3]):[0-5][0-9]')
        THEN epoch_ms(calendar(left(s, 19))) - (CASE WHEN s[20] = '-' THEN -1 ELSE 1 END)
             * (s[21:22]::BIGINT * 60 + s[24:25]::BIGINT) * 60000
END;

CREATE TABLE clean_events AS
WITH parsed AS (
    SELECT event_id, user_id, event_type, product_id, milliseconds(ts) AS event_ms,
           epoch_ms(calendar(left(ingest_ts, 19))) AS ingest_ms,
           CASE WHEN event_type = 'purchase' AND regexp_full_match(price_cents, '[0-9]+')
                THEN try_cast(price_cents AS BIGINT)
           END AS price_cents
    FROM events
    WHERE event_id IS NOT NULL
), projected AS (
    SELECT event_id, ingest_ms, coalesce(identities.person, user_id) AS person,
           CASE WHEN user_id IS NOT NULL AND event_type IN ('view', 'cart', 'purchase')
                     AND product_id IS NOT NULL AND event_ms >= start_ms AND event_ms < end_ms
                     AND ingest_ms - event_ms <= 86400000
                     AND (event_type <> 'purchase' OR price_cents BETWEEN 1 AND 10000000)
                THEN event_ms
           END AS event_ms,
           price_cents, CASE WHEN event_type = 'purchase' THEN coalesce(category, 'unknown') END AS category
    FROM parsed
    CROSS JOIN limits
    LEFT JOIN identities ON identities.id = parsed.user_id
    LEFT JOIN products USING (product_id)
), earliest AS (
    -- Keep invalid copies eligible for deduplication. The winning copy's
    -- timestamp is null if it failed any event rule.
    SELECT arg_min(struct_pack(person := person, event_ms := event_ms,
                               price_cents := price_cents, category := category), ingest_ms) AS e
    FROM projected GROUP BY event_id
)
SELECT e.* FROM earliest WHERE e.event_ms IS NOT NULL;
DROP VIEW events;

-- Rank distinct timestamps first. Equal-time events must all inherit the same session.
CREATE TABLE session_events AS
WITH timestamp_breaks AS (
    SELECT person, event_ms,
           lag(event_ms) OVER (PARTITION BY person ORDER BY event_ms) AS previous_ms
    FROM (SELECT DISTINCT person, event_ms FROM clean_events)
), timestamp_sessions AS (
    SELECT person, event_ms, max(CASE WHEN previous_ms IS NULL OR event_ms - previous_ms > 1800000
                                    THEN event_ms END)
             OVER (PARTITION BY person ORDER BY event_ms ROWS UNBOUNDED PRECEDING) AS session_ms
    FROM timestamp_breaks
)
SELECT person, session_ms, make_timestamp_ms(session_ms)::DATE AS day, price_cents, category
FROM clean_events JOIN timestamp_sessions USING (person, event_ms);
DROP TABLE clean_events;

CREATE TABLE sessions AS
SELECT person, session_ms, day, count(*) AS events, coalesce(sum(price_cents), 0)::BIGINT AS revenue_cents,
       count(price_cents) > 0 AS purchased
FROM session_events GROUP BY person, session_ms, day;

CREATE TABLE user_daily AS
WITH categories AS (
    SELECT person, day, category, sum(price_cents) AS category_revenue
    FROM session_events WHERE category IS NOT NULL GROUP BY person, day, category
), top_categories AS (
    SELECT person, day, first(category ORDER BY category_revenue DESC, category) AS top_category
    FROM categories GROUP BY person, day
), totals AS (
    SELECT person, day, count(*) AS sessions, sum(events)::BIGINT AS events,
           sum(revenue_cents)::BIGINT AS revenue_cents
    FROM sessions GROUP BY person, day
)
SELECT person AS user_id, day, sessions, events, revenue_cents, top_category
FROM totals LEFT JOIN top_categories USING (person, day);
DROP TABLE session_events;

CREATE TABLE clean_clicks AS
WITH parsed AS (
    SELECT DISTINCT click_id, user_id, campaign_id, milliseconds(ts) AS click_ms
    FROM clicks WHERE click_id IS NOT NULL AND user_id IS NOT NULL
)
SELECT click_id, coalesce(identities.person, user_id) AS person, campaign_id, click_ms
FROM parsed
CROSS JOIN limits
JOIN campaigns USING (campaign_id)
LEFT JOIN identities ON identities.id = parsed.user_id
WHERE click_ms >= start_ms - 604800000 AND click_ms < end_ms;
DROP VIEW clicks;

CREATE TABLE campaign_daily AS
WITH last_click_candidates AS (
    SELECT person, click_ms, arg_min(campaign_id, click_id) AS campaign_id
    FROM clean_clicks GROUP BY person, click_ms
), credited_sessions AS (
    SELECT CASE WHEN session_ms - click_ms <= 604800000 THEN campaign_id ELSE 'organic' END AS campaign,
           day, revenue_cents, purchased
    FROM sessions ASOF LEFT JOIN last_click_candidates
        ON sessions.person = last_click_candidates.person AND session_ms >= click_ms
), credit AS (
    SELECT campaign AS campaign_id, day, count(*) AS sessions, count(*) FILTER (WHERE purchased) AS purchasing_sessions,
           sum(revenue_cents)::BIGINT AS revenue_cents
    FROM credited_sessions GROUP BY campaign, day
), assists AS (
    SELECT campaign_id, day, count(*) AS assisted_sessions
    FROM (
        SELECT DISTINCT campaign_id, day, sessions.person, session_ms
        FROM sessions JOIN clean_clicks ON sessions.person = clean_clicks.person
            AND session_ms BETWEEN click_ms AND click_ms + 604800000
    )
    GROUP BY campaign_id, day
)
SELECT campaign_id, coalesce(campaigns.channel, 'organic') AS channel, day, coalesce(sessions, 0) AS sessions,
       coalesce(purchasing_sessions, 0) AS purchasing_sessions, coalesce(revenue_cents, 0) AS revenue_cents,
       coalesce(assisted_sessions, 0) AS assisted_sessions
FROM credit FULL JOIN assists USING (campaign_id, day)
LEFT JOIN campaigns USING (campaign_id);
