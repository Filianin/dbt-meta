"""Anonymized real-shape Power BI M-expression fixtures.

Structures are copied verbatim from a live scanResult dump; only project / schema /
table / column names were replaced with fictitious values. Each constant covers one
real variant the parser must handle.
"""

import json as _json

# 1:1 navigation import (GoogleBigQuery.Database navigation through Schema -> Table)
NAV_TABLE = '''let
    Source = GoogleBigQuery.Database(),
    #"my-project" = Source{[Name="my-project"]}[Data],
    core_utils_Schema = #"my-project"{[Name="core_utils",Kind="Schema"]}[Data],
    d_calendar_Table = core_utils_Schema{[Name="d_calendar",Kind="Table"]}[Data]
in
    d_calendar_Table'''

# 1:1 navigation import to a VIEW (Kind="View")
NAV_VIEW = '''let
    Source = GoogleBigQuery.Database(),
    #"my-project" = Source{[Name="my-project"]}[Data],
    report_x_Schema = #"my-project"{[Name="report_x",Kind="Schema"]}[Data],
    v_sales_View = report_x_Schema{[Name="v_sales",Kind="View"]}[Data]
in
    v_sales_View'''

# Native SQL, single table, newlines encoded as #(lf), backtick-quoted 3-part name
NATIVE_SINGLE = (
    'let\n    Source = Value.NativeQuery(GoogleBigQuery.Database()'
    '{[Name="my-project"]}[Data], "SELECT  client_id,#(lf)current_country,#(lf)'
    'registration_time#(lf)FROM `my-project.core_client.client_info`#(lf)'
    "WHERE registration_time >= '2023-01-01'\", null, [EnableFolding=true])\nin\n    Source"
)

# Native SQL with a LEFT JOIN across two tables
NATIVE_JOIN = (
    'let\n    Source = Value.NativeQuery(GoogleBigQuery.Database()'
    '{[Name="my-project"]}[Data], "SELECT urp.tr_client_id, ci.current_country#(lf)'
    'FROM `my-project.core_client.client_registration_utm_params` AS urp#(lf)'
    'LEFT JOIN `my-project.core_client.client_info` AS ci#(lf)'
    '    ON urp.tr_client_id = ci.client_id#(lf)'
    "WHERE urp.registration_date >= '2025-01-01'\", null, [EnableFolding=true])\nin\n    Source"
)

# Native SQL where the 3-part name is UNQUOTED and the project id has hyphens
NATIVE_UNQUOTED_HYPHEN = (
    'let\n    Source = Value.NativeQuery(GoogleBigQuery.Database()'
    '{[Name="my-project"]}[Data], "SELECT * FROM my-project.staging_amas.profiles_legal_owners",'
    ' null, [EnableFolding=true])\nin\n    Source'
)

# Native SQL followed by a Table.NestedJoin against ANOTHER in-dataset query
NATIVE_NESTED_JOIN = (
    'let\n    Source = Value.NativeQuery(GoogleBigQuery.Database()'
    '{[Name="my-project"]}[Data], "select profile_id, country_id#(lf)'
    'FROM my-project.core_amas.client_profiles_real", null, [EnableFolding=true]),\n'
    '    #"Merged Queries" = Table.NestedJoin(Source, {"country_id"}, '
    'client_model_countries, {"country_id"}, "client_model_countries", JoinKind.LeftOuter)\n'
    'in\n    #"Merged Queries"'
)

# Inline constant table (no BigQuery source at all)
INLINE_FROM_ROWS = '''let
    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45W8k1NLC4tSlWKjQUA", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Measure = _t]),
    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Measure", type text}})
in
    #"Changed Type"'''

# Pure DAX calculated table (no M source)
DAX_CALCULATED = '''SELECTCOLUMNS(
    CALENDAR(TODAY() - 365, TODAY()),
    "Date", [Date]
)'''

# ---------------------------------------------------------------------------
# PBIR-Legacy report.json (classic single-file layout from Fabric
# getDefinition?format=PBIR-Legacy). Shape copied from a live dump; all names
# are fictitious. `config` is a JSON-encoded string exactly as PBIR stores it.
# ---------------------------------------------------------------------------


def _config(visual_type, projections):
    """Build the JSON-string `config` of a visualContainer."""
    return _json.dumps(
        {"singleVisual": {"visualType": visual_type, "projections": projections}}
    )


# A funnel + a slicer + a chart with a legend breakdown — the canonical
# "Stage funnel, Device slicer, no OS split" example from the plan.
PBIR_LEGACY_REPORT = {
    "sections": [
        {
            "name": "ReportSection1",
            "displayName": "PPC Reg Cohorts",
            "visualContainers": [
                {
                    "config": _config(
                        "funnel",
                        {
                            "Values": [{"queryRef": "Sum(events.stage_count)"}],
                            "Category": [{"queryRef": "events.stage"}],
                        },
                    )
                },
                {
                    "config": _config(
                        "slicer",
                        {"Values": [{"queryRef": "Device.device_type"}]},
                    )
                },
                {
                    "config": _config(
                        "stackedAreaChart",
                        {
                            "Y": [{"queryRef": "Count(events.client_id)"}],
                            "Category": [{"queryRef": "d_calendar.day_iso"}],
                            "Series": [{"queryRef": "Device.device_type"}],
                            "Tooltips": [{"queryRef": "events.deposits"}],
                        },
                    )
                },
            ],
        },
        {
            "name": "ReportSection2",
            "displayName": "Detail",
            "visualContainers": [
                {
                    "config": _config(
                        "customCardVisual",
                        {"Custom Role": [{"queryRef": "metrics.total_revenue"}]},
                    )
                }
            ],
        },
    ]
}

# ---------------------------------------------------------------------------
# PBIR-Legacy filter + title fixtures (v0.3.6 visual semantics). `filters` at
# every scope is a JSON-string array exactly as PBIR stores it; titles live in
# `singleVisual.vcObjects.title`. All names fictitious.
# ---------------------------------------------------------------------------


def _col_expr(entity, prop, source_alias="t"):
    """A Column field expression as it appears in a filter's `expression`."""
    return {
        "Column": {
            "Expression": {"SourceRef": {"Entity": entity, "Source": source_alias}},
            "Property": prop,
        }
    }


def _filter_in(entity, prop, values, ftype="Categorical"):
    return {
        "name": f"f_{prop}",
        "expression": _col_expr(entity, prop),
        "type": ftype,
        "filter": {
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [_col_expr(entity, prop)],
                            "Values": [
                                [{"Literal": {"Value": f"'{v}'"}}] for v in values
                            ],
                        }
                    }
                }
            ]
        },
    }


def _filter_cmp(entity, prop, kind_num, bound, ftype="Advanced"):
    return {
        "name": f"f_{prop}",
        "expression": _col_expr(entity, prop),
        "type": ftype,
        "filter": {
            "Where": [
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": kind_num,
                            "Left": _col_expr(entity, prop),
                            "Right": {"Literal": {"Value": bound}},
                        }
                    }
                }
            ]
        },
    }


def _filter_relative(entity, prop, count, time_unit=0):
    return {
        "name": f"f_{prop}",
        "expression": _col_expr(entity, prop),
        "type": "RelativeDate",
        "filter": {
            "Where": [
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": 2,
                            "Left": _col_expr(entity, prop),
                            "Right": {
                                "DateSpan": {
                                    "Expression": {"Literal": {"Value": f"{count}L"}},
                                    "TimeUnit": time_unit,
                                }
                            },
                        }
                    }
                }
            ]
        },
    }


def _filter_topn(entity, prop, n, by_entity, by_prop, func=0):
    return {
        "name": f"f_{prop}",
        "expression": _col_expr(entity, prop),
        "type": "TopN",
        "filter": {
            "Where": [
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": 3,
                            "Left": _col_expr(entity, prop),
                            "Right": {"Literal": {"Value": f"{n}L"}},
                        }
                    }
                }
            ],
            "OrderBy": [
                {
                    "Direction": 2,
                    "Expression": {
                        "Aggregation": {
                            "Function": func,
                            "Expression": _col_expr(by_entity, by_prop),
                        }
                    },
                }
            ],
        },
    }


def _filter_advanced(entity, prop):
    return {
        "name": "f_adv",
        "expression": _col_expr(entity, prop),
        "type": "Advanced",
        "filter": {
            "Where": [
                {
                    "Condition": {
                        "And": {
                            "Left": {
                                "Comparison": {
                                    "ComparisonKind": 0,
                                    "Left": _col_expr(entity, "status"),
                                    "Right": {"Literal": {"Value": "'active'"}},
                                }
                            },
                            "Right": {
                                "Comparison": {
                                    "ComparisonKind": 1,
                                    "Left": _col_expr(entity, "amount"),
                                    "Right": {"Literal": {"Value": "0L"}},
                                }
                            },
                        }
                    }
                }
            ]
        },
    }


def _visual_container(visual_type, projections, title=None, filters=None):
    """Build a visualContainer with optional explicit title + visual-level filters."""
    single = {"singleVisual": {"visualType": visual_type, "projections": projections}}
    if title is not None:
        single["singleVisual"]["vcObjects"] = {
            "title": [
                {"properties": {"text": {"expr": {"Literal": {"Value": f"'{title}'"}}}}}
            ]
        }
    container = {"config": _json.dumps(single)}
    if filters is not None:
        container["filters"] = _json.dumps(filters)
    return container


# One report exercising every filter scope + op + an explicit title.
PBIR_LEGACY_SEMANTICS = {
    "filters": _json.dumps(
        [
            _filter_in("Device", "device_type", ["desktop", "mobile"]),
            _filter_advanced("events", "flags"),
        ]
    ),
    "sections": [
        {
            "name": "S1",
            "displayName": "Overview",
            "filters": _json.dumps([_filter_relative("d_calendar", "day_iso", 30, 0)]),
            "visualContainers": [
                _visual_container(
                    "barChart",
                    {
                        "Y": [{"queryRef": "Sum(events.revenue)"}],
                        "Category": [{"queryRef": "events.stage"}],
                    },
                    title="Revenue by stage",
                    filters=[_filter_cmp("events", "amount", 1, "100L")],
                ),
                _visual_container(
                    "table",
                    {"Values": [{"queryRef": "events.client_id"}]},
                    filters=[_filter_topn("events", "client_id", 10, "events", "revenue", 0)],
                ),
                _visual_container(
                    "card",
                    {"Values": [{"queryRef": "Count(events.client_id)"}]},
                ),
            ],
        }
    ],
}


# Malformed containers the parser must survive: bad JSON config, missing
# singleVisual, an empty section.
PBIR_LEGACY_MESSY = {
    "sections": [
        {
            "name": "S1",
            "displayName": "Messy",
            "visualContainers": [
                {"config": "not valid json{{"},
                {"config": _json.dumps({"noSingleVisual": True})},
                {},
                {
                    "config": _config(
                        "card", {"Values": [{"queryRef": "events.ok"}]}
                    )
                },
            ],
        },
        {"name": "S2", "displayName": "Empty", "visualContainers": []},
    ]
}
