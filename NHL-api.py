from nhlpy.api.query.builder import QueryBuilder, QueryContext
from nhlpy.nhl_client import NHLClient
from nhlpy.api.query.filters.draft import DraftQuery
from nhlpy.api.query.filters.season import SeasonQuery
from nhlpy.api.query.filters.game_type import GameTypeQuery
from nhlpy.api.query.filters.position import PositionQuery, PositionTypes
import json

client = NHLClient()

filters = [
    SeasonQuery(season_start="20232024", season_end="20242025"),
    PositionQuery(position=PositionTypes.DEFENSE)
]

query_builder = QueryBuilder()
query_context: QueryContext = query_builder.build(filters=filters)

skaterStats = client.stats.skater_stats_with_query_context(
    report_type='summary',
    query_context=query_context,
    aggregate=True
)

with open("skater_stats.json", "w") as f:
    json.dump(skaterStats, f, indent=4)  # indent for readability