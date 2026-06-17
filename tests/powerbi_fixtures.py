"""Anonymized real-shape Power BI M-expression fixtures.

Structures are copied verbatim from a live scanResult dump; only project / schema /
table / column names were replaced with fictitious values. Each constant covers one
real variant the parser must handle.
"""

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
