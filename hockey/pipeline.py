import sqlite3
import nhlAPI
from concurrent.futures import ThreadPoolExecutor
import hockey.databaseManager as dbm
import hockey.ingestion_espn as ingESPN
import hockey.ingestion_ff as ingFF
import hockey.ingestion_yahoo as ingYAH
import hockey.ingestion_nhl as ingNHL
import hockey.scoring as hSc
from hockey.config import ff_leagueID

# Run all FleaFlicker related functions
def helpFlea(debug=False):
    ingFF.build_ff_indexes()
    if debug==True:
        print("✅ helpFlea built indices")
    ingFF.apiScoringGet(ff_leagueID)
    if debug==True:
        print("✅ helpFlea retrieved scoring")
    dbm.dbScoringPop()
    if debug==True:
        print("✅ helpFlea populated scoring")

# Run all NHL API calls, recently CUT index building. CALL INDEX BUILDING SEPARATELY
def helpNHL(debug=False, on_status=None):
    report = on_status or (lambda message: None)
    report("Fetching NHL skater and goalie statistics…")
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_rawSkate = ex.submit(nhlAPI.rawstats_dynamic_skater)
        fut_rawGoal = ex.submit(nhlAPI.rawstats_dynamic_goalie)
        fut_rawSkate.result()
        fut_rawGoal.result()
        if debug==True:
            print("✅ helpNHL TPE workers done")
    report("Updating NHL-to-fantasy player mappings…")
    dbm.dbPlayerIndexLocalPop()
    if debug==True:
        print("✅ helpNHL populated local index")
    report("Fetching hits, blocks, and ice time…")
    ingNHL.dbPopulateRealtime()
    if debug==True:
        print("✅ helpNHL populated realtime")

def buildStatsBasic(debug=False, on_status=None):
    """Refresh statistics; optional callbacks also support a web progress display."""
    report = on_status or (lambda message: None)
    helpNHL(debug, on_status=report)
    report("Refreshing FleaFlicker league rosters…")
    ingFF.ff_apiGetRosters(ff_leagueID)
    report("Loading fantasy scoring rules…")
    dbm.dbScoringPop()
    report("Rebuilding fantasy rankings…")
    hSc.dbBuildUnifiedFantasyView(False)
    report("Statistics and availability refreshed.")

    if debug:
        print("✅ buildStatsBasic done")
