import os
from nhlpy import NHLClient
from nhlpy.api.query.builder import QueryBuilder, QueryContext
from nhlpy.api.query.filters.franchise import FranchiseQuery
from nhlpy.api.query.filters.season import SeasonQuery
import time
import json
import scoringDatabaseBuilder as sdb

client = NHLClient()

# Get available filter options and API configuration
config = client.misc.config()