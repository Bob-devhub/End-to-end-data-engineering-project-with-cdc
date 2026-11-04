# Event simulator

Sends synthetic retail events to an Eventstream **custom endpoint** source.

1. In Fabric, create Eventstream `es_retail_events` → **Add source → Custom endpoint**.
2. Open the custom endpoint → **Event Hub** tab → copy the connection string (with `EntityPath`).
3. Add a destination: **Eventhouse** → KQL DB `kql_retail_rt` → table `events_raw`,
   format JSON, mapping `events_raw_mapping` (created by `scripts/deploy_kql.py --apply`).

```bash
pip install -e ".[sim]"
export EVENTSTREAM_CONNECTION_STRING="Endpoint=sb://...;EntityPath=..."
python simulator/producer.py --rate 20 --freezer-fault-store 7
```

`--dry-run --count 10` prints events to stdout without sending anything.
