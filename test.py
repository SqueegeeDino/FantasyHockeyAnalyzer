from nhlpy import NHLClient

skaterStats = NHLClient.skater_stats.skater_stats_summary_simple(
    player_id="8478402", # ID is currently fixed to Connor McDavid
    start_season='20242025',
    end_season = '20242025'
    )