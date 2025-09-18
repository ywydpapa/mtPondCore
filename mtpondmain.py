import topuprise
import json

snapshot = topuprise.uprises()
snapshot = json.loads(snapshot)
intersection = snapshot.get("multi_tf_intersection", {})
items = intersection.get("items", [])
result = [
    {"market": it["market"], "avg_score": it["avg_score"]}
    for it in items
]
print(result)


