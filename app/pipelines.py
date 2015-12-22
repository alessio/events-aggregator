

def aggregate_events(object_id, start, end):
    return [
        {
            "$match": {
                "object_id": object_id,
                "time": {"$lte": end, "$gte": start},
            }
        },
        {
            "$group": {
                "_id": {"type": "$type", "time": "$time"},
                "count": {"$sum": 1}
            },
        },
        {
            "$project": {
                "_id": 0,
                "time": "$_id.time",
                "type": "$_id.type",
                "object_id": 1,
                "count": 1,
            }
        },
        {
            "$group": {
                "_id": {"type": "$type"},
                "events": {
                    "$push": {
                        "events": "$count", "time": "$time"
                    }
                }
            },
        },
        {
            "$project": {
                "_id": 0,
                "type": "$_id.type",
                "events": 1,
            }
        },
    ]
