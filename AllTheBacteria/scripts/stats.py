from atb_sketcher.db import DB
import json, sys
db = DB("/scratch/dmk333_new/known_microbe_hashes/AllTheBacteria/data/state/atb_sketcher.sqlite")
print(json.dumps(db.stats(), indent=2))
