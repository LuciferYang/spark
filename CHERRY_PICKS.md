# Cherry-Picked PRs Tracking

## Branch: spark-40193-on-upmaster

Base: upmaster (420918872b1)

### Prerequisites (already in upmaster)

| JIRA | Commit | Title |
|---|---|---|
| [SPARK-54136](https://issues.apache.org/jira/browse/SPARK-54136) | a871ba4464e | Extract plan merging logic from MergeScalarSubqueries to PlanMerger |
| [SPARK-44571](https://issues.apache.org/jira/browse/SPARK-44571) | 78fcc934d31 | Merge subplans with one row result |

### Changes in this branch

| # | Source | Title | Commit |
|---|---|---|---|
| 1 | [PR #37630](https://github.com/apache/spark/pull/37630) (closed) | SPARK-40193: Merge subquery plans with different filters | 0e3630be18c |
| 2 | Custom extension | Filter propagation through Inner/Cross joins | 0e3630be18c |

### TPC-DS Plan Changes

| Query | Before | After | Mechanism |
|---|---|---|---|
| **q9** | 15 store_sales scans | 1 merged scan + FILTER | Filter propagation (scalar subqueries) |
| **q28** | 6 store_sales scans + 5 cross joins | 1 merged scan + FILTER + Expand | Non-grouping agg merge + filter propagation |
| **q88** | 16 store_sales scans | 2 scans (8 subqueries merged) | Filter propagation through joins |
| **q90** | 4 web_sales scans | 2 scans (2 subqueries merged) | Filter propagation through joins |
| **q2** | Duplicate date-filtered scans | Merged with FILTER | Filter propagation |
| **q59** | Duplicate week-filtered scans | Merged with FILTER | Filter propagation |
