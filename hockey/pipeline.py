import sqlite3
import nhlAPI
from concurrent.futures import ThreadPoolExecutor
import hockey.databaseManager as dbm
import hockey.ingestion_espn as ingESPN
import hockey.ingestion_ff as ingFF
import hockey.ingestion_yahoo as ingYAH
import hockey.ingestion_nhl as ingNHL
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

# Run all NHL API calls, currently includes local Index
def helpNHL(debug=False):
    with ThreadPoolExecutor(max_workers=3) as ex:
        fut_index = ex.submit(ingNHL.dbPlayerIndexNHLPop)
        fut_rawSkate = ex.submit(nhlAPI.rawstats_dynamic_skater)
        fut_rawGoal = ex.submit(nhlAPI.rawstats_dynamic_goalie)
        fut_index.result()
        fut_rawSkate.result()
        fut_rawGoal.result()
        if debug==True:
            print("✅ helpNHL TPE workers done")
    dbm.dbPlayerIndexLocalPop()
    if debug==True:
        print("✅ helpNHL populated local index")
    ingNHL.dbPopulateRealtime()
    if debug==True:
        print("✅ helpNHL populated realtime")