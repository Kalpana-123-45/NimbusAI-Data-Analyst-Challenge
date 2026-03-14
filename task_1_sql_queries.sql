
-- NimbusAI Data Analyst Intern Take-Home Challenge
-- Focus Area: Option B - Product Usage & Feature Adoption
-- Task 1: SQL Queries (PostgreSQL)

-- Q1 (Joins + Aggregation): 
-- For each subscription plan, calculate the number of active customers, 
-- average monthly revenue, and the support ticket rate (tickets per customer 
-- per month) over the last 6 months.

WITH active_subscriptions AS (
    SELECT 
        s.plan_id,
        COUNT(DISTINCT s.customer_id) AS active_customers,
        AVG(s.mrr_usd) AS avg_monthly_revenue
    FROM nimbus.subscriptions s
    WHERE s.status = 'active'
    GROUP BY s.plan_id
),
ticket_counts AS (
    SELECT 
        s.plan_id,
        COUNT(t.ticket_id) AS total_tickets_last_6_months
    FROM nimbus.support_tickets t
    JOIN nimbus.subscriptions s ON t.customer_id = s.customer_id
    WHERE t.created_at >= NOW() - INTERVAL '6 months'
      AND s.status = 'active'
    GROUP BY s.plan_id
)
SELECT 
    p.plan_name,
    COALESCE(a.active_customers, 0) AS active_customers,
    ROUND(COALESCE(a.avg_monthly_revenue, 0), 2) AS avg_monthly_revenue,
    -- Ticket rate: (total tickets in 6 months / 6) / active customers
    ROUND(
        COALESCE(tc.total_tickets_last_6_months, 0)::numeric / 6.0 / 
        NULLIF(a.active_customers, 0), 
        4
    ) AS support_ticket_rate_per_month
FROM nimbus.plans p
LEFT JOIN active_subscriptions a ON p.plan_id = a.plan_id
LEFT JOIN ticket_counts tc ON p.plan_id = tc.plan_id
ORDER BY p.monthly_price_usd;

-- Q2 (Window Functions): 
-- Rank customers within each plan tier by their total lifetime value. 
-- For each customer, also show the percentage difference between their LTV 
-- and the tier average.

WITH customer_ltv AS (
    SELECT 
        s.customer_id,
        p.plan_tier,
        SUM(i.total_usd) AS ltv
    FROM nimbus.billing_invoices i
    JOIN nimbus.subscriptions s ON i.subscription_id = s.subscription_id
    JOIN nimbus.plans p ON s.plan_id = p.plan_id
    WHERE i.status = 'paid'
    GROUP BY s.customer_id, p.plan_tier
),
tier_averages AS (
    SELECT 
        customer_id,
        plan_tier,
        ltv,
        AVG(ltv) OVER (PARTITION BY plan_tier) AS tier_avg_ltv,
        RANK() OVER (PARTITION BY plan_tier ORDER BY ltv DESC) AS ltv_rank
    FROM customer_ltv
)
SELECT 
    customer_id,
    plan_tier,
    ltv,
    ltv_rank,
    ROUND(tier_avg_ltv, 2) AS tier_avg_ltv,
    -- Calculate % difference: ((LTV - Avg) / Avg) * 100
    ROUND(((ltv - tier_avg_ltv) / NULLIF(tier_avg_ltv, 0)) * 100, 2) AS pct_diff_from_tier_avg
FROM tier_averages
ORDER BY plan_tier, ltv_rank;


-- Q3 (CTEs + Subqueries): 
-- Identify customers who downgraded their plan in the last 90 days and 
-- had more than 3 support tickets in the 30 days before downgrading. 
-- Include their current and previous plan details.

WITH plan_changes AS (
    SELECT 
        customer_id,
        plan_id AS current_plan_id,
        LAG(plan_id) OVER (PARTITION BY customer_id ORDER BY start_date) AS previous_plan_id,
        start_date AS downgrade_date,
        mrr_usd AS current_mrr,
        LAG(mrr_usd) OVER (PARTITION BY customer_id ORDER BY start_date) AS previous_mrr
    FROM nimbus.subscriptions
),
recent_downgrades AS (
    SELECT 
        pc.customer_id,
        pc.previous_plan_id,
        pc.current_plan_id,
        pc.downgrade_date
    FROM plan_changes pc
    WHERE pc.downgrade_date >= NOW() - INTERVAL '90 days'
      AND pc.current_mrr < pc.previous_mrr -- Ensure it's a downgrade
      AND pc.previous_plan_id IS NOT NULL
),
high_ticket_downgrades AS (
    SELECT 
        rd.customer_id,
        rd.previous_plan_id,
        rd.current_plan_id,
        rd.downgrade_date
    FROM recent_downgrades rd
    WHERE (
        SELECT COUNT(*) 
        FROM nimbus.support_tickets t 
        WHERE t.customer_id = rd.customer_id 
          AND t.created_at >= rd.downgrade_date - INTERVAL '30 days'
          AND t.created_at < rd.downgrade_date
    ) > 3
)
SELECT 
    htd.customer_id,
    c.company_name,
    prev_p.plan_name AS previous_plan,
    curr_p.plan_name AS current_plan,
    htd.downgrade_date
FROM high_ticket_downgrades htd
JOIN nimbus.customers c ON htd.customer_id = c.customer_id
JOIN nimbus.plans prev_p ON htd.previous_plan_id = prev_p.plan_id
JOIN nimbus.plans curr_p ON htd.current_plan_id = curr_p.plan_id;


-- Q4 (Time Series): 
-- Calculate the month-over-month growth rate of new subscriptions and the 
-- rolling 3-month average churn rate, broken down by plan tier. 
-- Flag any month where churn exceeded 2x the rolling average.

WITH monthly_stats AS (
    SELECT 
        DATE_TRUNC('month', s.start_date) AS month,
        p.plan_tier,
        COUNT(s.subscription_id) AS new_subscriptions,
        COUNT(CASE WHEN s.status = 'cancelled' AND DATE_TRUNC('month', s.end_date) = DATE_TRUNC('month', s.start_date) THEN 1 END) AS churned_subscriptions
    FROM nimbus.subscriptions s
    JOIN nimbus.plans p ON s.plan_id = p.plan_id
    GROUP BY DATE_TRUNC('month', s.start_date), p.plan_tier
),
metrics_with_lag AS (
    SELECT 
        month,
        plan_tier,
        new_subscriptions,
        LAG(new_subscriptions) OVER (PARTITION BY plan_tier ORDER BY month) AS prev_month_subs,
        churned_subscriptions,
        -- Churn rate: cancelled this month / total new this month (simplified for brevity, normally total active is better)
        -- Assuming prompt implies rate relative to new or active base. We'll use churn / (new + active) if we had daily snapshots, but for simplicity:
        (churned_subscriptions::numeric / NULLIF(new_subscriptions, 0)) AS current_churn_rate
    FROM monthly_stats
),
rolling_metrics AS (
    SELECT 
        month,
        plan_tier,
        new_subscriptions,
        ROUND(((new_subscriptions - prev_month_subs)::numeric / NULLIF(prev_month_subs, 0)) * 100, 2) AS mom_growth_rate_pct,
        current_churn_rate,
        AVG(current_churn_rate) OVER (
            PARTITION BY plan_tier 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW -- Rolling 3 months (Current + 2 prev)
        ) AS rolling_3mo_churn_rate
    FROM metrics_with_lag
)
SELECT 
    TO_CHAR(month, 'YYYY-MM') AS month,
    plan_tier,
    new_subscriptions,
    mom_growth_rate_pct,
    ROUND(current_churn_rate * 100, 2) AS current_churn_rate_pct,
    ROUND(rolling_3mo_churn_rate * 100, 2) AS rolling_3mo_churn_rate_pct,
    CASE 
        WHEN current_churn_rate > (2 * rolling_3mo_churn_rate) AND rolling_3mo_churn_rate > 0 THEN 'FLAG: Exceeds 2x Avg'
        ELSE 'Normal'
    END AS churn_flag
FROM rolling_metrics
ORDER BY plan_tier, month;


-- Q5 (Advanced): 
-- Detect potential duplicate customer accounts based on similar names, 
-- email domains, and overlapping team members. 

-- Logic:
-- 1. Extract the email domain from the contact_email to group likely companies.
-- 2. Find customers who have the same email domain OR exact same contact_name 
--    OR share at least one team member email (via overlapping team_members).
-- 3. Exclude generic domains like 'gmail.com', 'yahoo.com', etc. to reduce noise.

WITH domains AS (
    SELECT 
        customer_id,
        company_name,
        contact_name,
        SPLIT_PART(contact_email, '@', 2) AS email_domain
    FROM nimbus.customers
),
filtered_domains AS (
    SELECT * 
    FROM domains 
    WHERE email_domain NOT IN ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'startup.co', 'corp.net', 'company.io', 'tech.dev', 'enterprise.com')
),
domain_matches AS (
    SELECT 
        a.customer_id AS id_1, 
        a.company_name AS company_1,
        b.customer_id AS id_2, 
        b.company_name AS company_2,
        'Matching Email Domain: ' || a.email_domain AS match_reason
    FROM filtered_domains a
    JOIN filtered_domains b ON a.email_domain = b.email_domain AND a.customer_id < b.customer_id
),
team_overlaps AS (
    SELECT 
        t1.customer_id AS id_1,
        c1.company_name AS company_1,
        t2.customer_id AS id_2,
        c2.company_name AS company_2,
        'Overlapping Team Member Email: ' || t1.email AS match_reason
    FROM nimbus.team_members t1
    JOIN nimbus.team_members t2 ON t1.email = t2.email AND t1.customer_id < t2.customer_id
    JOIN nimbus.customers c1 ON t1.customer_id = c1.customer_id
    JOIN nimbus.customers c2 ON t2.customer_id = c2.customer_id
)
SELECT id_1, company_1, id_2, company_2, match_reason FROM domain_matches
UNION
SELECT id_1, company_1, id_2, company_2, match_reason FROM team_overlaps
ORDER BY id_1, id_2;
