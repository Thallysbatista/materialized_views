# BI Case Studies

This document provides detailed context, logic, and business impact of selected SQL views from the repository.

---

## `mvw_followup_responsible_by_sector`

**Goal:**  
Track the person responsible for each sector change during a sale (`followup_id`) and determine whether that change was part of a valid funnel transition.

**Context:**  
In the sales process, multiple users (agents, systems) are responsible for moving a sale from one sector (e.g., SALES, TYPING) to another. Tracking who handled the transition at each step allowed the company to improve accountability and identify operational bottlenecks.

**Highlights:**
- Identifies transitions like SALES → TYPING → FORMALIZATION.
- Uses `LAG()` to capture the previous stage responsible.
- Includes a field `is_conversion` to highlight valid funnel progressions.

**Impact:**  
Helped managers understand where deals were stuck and which users consistently pushed sales forward.

---

## `mvw_followups_sector_sla`

**Goal:**  
Calculate the amount of time each sale spent in each sector of the sales pipeline to monitor SLA compliance and operational efficiency.

**Context:**  
Sales go through predefined steps like SALES → TYPING → FORMALIZATION → PROGRESS → COMPLETED. Understanding how long each sale stays in a given sector can reveal process inefficiencies or backlogs.

**Highlights:**
- Computes the time (in seconds) spent in each sector per sale.
- Uses `LEAD()` to determine the start and end of each sector phase.
- Includes logic to stop counting once the sale reaches the `COMPLETED` stage.

**Impact:**  
The team was able to detect unusually long durations in specific sectors, enabling process optimization and SLA monitoring.

---

## `mvw_user_first_vs_repeat_purchase`

**Goal:**  
Identify whether a completed sale for each user was their first or a repeat purchase.

**Context:**  
For product and retention analysis, it’s critical to know whether a sale came from a new customer or a returning one. This allows marketing and business teams to assess acquisition strategies and customer loyalty.

**Highlights:**
- Filters only `Finalizado` (completed) sales with valid commissions.
- Uses `ROW_NUMBER()` partitioned by user to identify the chronological order of purchases.
- Classifies each sale as `First-time purchase` or `Repeat purchase`.

**Impact:**  
Provided clear visibility into user purchasing behavior and allowed the marketing team to track retention over time, as well as measure the effectiveness of reactivation campaigns.

---

## `mvw_monthly_growth_metrics`

**Goal:**  
Monitor monthly business growth using cumulative and per-month metrics for new users, qualified leads, new clients, and total commissions.

**Context:**  
This view unifies all core KPIs into a single structure to support high-level business dashboards. It enables tracking acquisition velocity and user-to-client conversion rates.

**Highlights:**
- Tracks new and cumulative covenants, users, and clients.
- Calculates total commissions, including those from first-time purchases.
- Measures lead qualification rate monthly and over time.

**Impact:**  
Enabled stakeholders to evaluate business performance, growth velocity, and revenue from new clients, providing key inputs for strategic decisions.

---

## `mvw_user_funnel_daily_metrics`

**Goal:**  
Monitor user onboarding and sales funnel activity per day, including validation status, proposal activity, and sales conversion.

**Context:**  
This view connects user creation, lead validation, follow-up activity, and proposal generation into a single daily view. It removes incomplete or canceled transitions to avoid counting noise in the funnel.

**Highlights:**
- Identifies users with valid or blocked lead records.
- Tracks follow-ups created and finalized within 30 days.
- Measures how many users actually progressed in the funnel after registration.

**Impact:**  
Used to measure user engagement and conversion rate across acquisition channels and platforms. Key in optimizing user activation and identifying daily drop-offs.

