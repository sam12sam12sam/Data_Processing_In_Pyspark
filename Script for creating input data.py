import fastavro
from fastavro.schema import load_schema
import pandas as pd
import json

# ad_impressions_data data
ad_impressions_data = [
    {"ad_creative_id": "ad1", "user_id": "user1", "timestamp": "2024-05-27T10:00:00", "website": "site1"},
    {"ad_creative_id": "ad2", "user_id": "user2", "timestamp": "2024-05-27T10:05:00", "website": "site2"},
    {"ad_creative_id": "ad3", "user_id": "user3", "timestamp": "2024-05-27T10:10:00", "website": "site3"},
    {"ad_creative_id": "ad4", "user_id": "user4", "timestamp": "2024-05-27T10:15:00", "website": "site4"},
    {"ad_creative_id": "ad5", "user_id": "user5", "timestamp": "2024-05-27T10:20:00", "website": "site5"},
    {"ad_creative_id": "ad6", "user_id": "user6", "timestamp": "2024-05-27T10:25:00", "website": "site6"},
    {"ad_creative_id": "ad7", "user_id": "user7", "timestamp": "2024-05-27T10:30:00", "website": "site7"},
    {"ad_creative_id": "ad8", "user_id": "user8", "timestamp": "2024-05-27T10:35:00", "website": "site8"},
    {"ad_creative_id": "ad9", "user_id": "", "timestamp": "2024-05-27T10:40:00", "website": "site9"},
    {"ad_creative_id": "ad10", "user_id": "", "timestamp": "2024-05-27T10:45:00", "website": "site10"}
]

# Save as JSON file
with open('ad_impressions_data.json', 'w') as f:
    json.dump(ad_impressions_data, f, indent=4)


# clicks and conversions data 
clicks_conversions_data = {
    "event_timestamp": ["", "2024-05-27T10:06:00", "2024-05-27T10:12:00", "2024-05-27T10:16:00", "2024-05-27T10:22:00", "2024-05-27T10:26:00", "2024-05-27T10:32:00", "2024-05-27T10:36:00", "2024-05-27T10:42:00", "2024-05-27T10:46:00"],
    "user_id": ["user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "", "user10"],
    "ad_campaign_id": ["camp1", "camp2", "camp3", "camp4", "camp5", "camp6", "camp6", "camp9", "camp9", "camp9"],
    "conversion_type": ["signup", "purchase", "signup", "purchase", "signup", "purchase", "signup", "purchase", "signup", "purchase"]
}

# Convert to DataFrame
clicks_conversions_df = pd.DataFrame(clicks_conversions_data)

# Save as CSV file
clicks_conversions_df.to_csv('clicks_conversions_data.csv', index=False)


# bid_requests_data data

bid_requests_data = [
    {"user_id": "user1", "auction_id": "auction1", "ad_targeting_criteria": "criteria1", "timestamp": "2024-05-27T10:00:00"},
    {"user_id": "user2", "auction_id": "auction2", "ad_targeting_criteria": "criteria2", "timestamp": "2024-05-27T10:05:00"},
    {"user_id": "user3", "auction_id": "auction3", "ad_targeting_criteria": "criteria3", "timestamp": "2024-05-27T10:10:00"},
    {"user_id": "", "auction_id": "auction4", "ad_targeting_criteria": "criteria4", "timestamp": "2024-05-27T10:15:00"},
    {"user_id": "user5", "auction_id": "auction5", "ad_targeting_criteria": "criteria5", "timestamp": "2024-05-27T10:20:00"},
    {"user_id": "user6", "auction_id": "auction6", "ad_targeting_criteria": "criteria6", "timestamp": "2024-05-27T10:25:00"},
    {"user_id": "user6", "auction_id": "auction7", "ad_targeting_criteria": "criteria7", "timestamp": "2024-05-27T10:30:00"},
    {"user_id": "user9", "auction_id": "", "ad_targeting_criteria": "criteria8", "timestamp": "2024-05-27T10:35:00"},
    {"user_id": "user9", "auction_id": "auction9", "ad_targeting_criteria": "criteria9", "timestamp": "2024-05-27T10:40:00"},
    {"user_id": "user9", "auction_id": "auction10", "ad_targeting_criteria": "criteria10", "timestamp": ""},
]

# Define the schema
bid_requests_schema = {
    "type": "record",
    "name": "BidRequest",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "auction_id", "type": "string"},
        {"name": "ad_targeting_criteria", "type": "string"},
        {"name": "timestamp", "type": "string"}
    ]
}

# Write data to Avro file
with open('bid_requests_data.avro', 'wb') as f:
    fastavro.writer(f, bid_requests_schema, bid_requests_data)
