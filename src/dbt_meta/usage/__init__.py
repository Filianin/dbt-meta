"""Column-usage analysis (WHERE/JOIN/GROUP BY/...) for dbt models.

Where ``dbt_meta.lineage`` walks SELECT-lineage trees and answers "where does a
column come from", this module walks WHERE/JOIN/GROUP/ORDER/QUALIFY/window
clauses and answers "how do downstream models USE upstream columns".

The output drives three optimization advisors:
    advisor_refresh    — column-aware --full-refresh planner
    advisor_cluster    — clustering-key recommendations
    advisor_partition  — partition column recommendations
"""

from dbt_meta.usage.advisor_cluster import (
    ClusterAdvisor,
    ClusterAdvisorResult,
    ClusterRecommendation,
)
from dbt_meta.usage.advisor_partition import (
    PartitionAdvisor,
    PartitionAdvisorResult,
    PartitionRecommendation,
)
from dbt_meta.usage.advisor_refresh import (
    RefreshAdvisor,
    RefreshPlan,
    changed_models_from_git,
)
from dbt_meta.usage.extractor import ColumnUsageExtractor, UsageEvent

__all__ = [
    "ClusterAdvisor",
    "ClusterAdvisorResult",
    "ClusterRecommendation",
    "ColumnUsageExtractor",
    "PartitionAdvisor",
    "PartitionAdvisorResult",
    "PartitionRecommendation",
    "RefreshAdvisor",
    "RefreshPlan",
    "UsageEvent",
    "changed_models_from_git",
]
