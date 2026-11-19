"""
Shared configuration for the CS 498 Stage 3 prototype (Cloud Data Pioneers).

Everything that differs between a teammate's laptop and another's lives here and
can be overridden with environment variables, so no code changes are needed.
"""
import os

# Connection string for the MongoDB server. Default: local mongod on the standard port.
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")

# Database that holds the four collections (listings, calendar, reviews, neighbourhoods).
DB_NAME = os.environ.get("AIRBNB_DB", "airbnb")

# Folder that contains one sub-folder per city with the Inside Airbnb files.
DATA_DIR = os.environ.get("AIRBNB_DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))

# Sub-folder name  ->  city name stored in every document.
# The city name is what the queries filter on, so it must stay consistent.
CITIES = {
    "los-angeles": "Los Angeles",
    "portland": "Portland",
    "salem": "Salem",
    "san-diego": "San Diego",
}

# Batch size for insert_many. 10k documents keeps memory flat while still
# amortising the network round trip.
BATCH_SIZE = int(os.environ.get("AIRBNB_BATCH_SIZE", "10000"))
