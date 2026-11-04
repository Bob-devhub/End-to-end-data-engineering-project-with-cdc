import importlib.util
import random
from itertools import islice
from pathlib import Path

from cdc_framework.config import load_metadata

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("producer", ROOT / "simulator" / "producer.py")
producer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(producer)


def test_simulator_events_match_stream_metadata():
    """Every simulated event carries every field the KQL parse functions expect."""
    streams = {s.event_type: s for s in load_metadata(ROOT / "metadata" / "config").streams}
    events = list(islice(producer.generate(random.Random(1), fault_store=None), 500))
    assert {e["event_type"] for e in events} == set(streams)
    for e in events:
        expected = {f.path.removeprefix("$.") for f in streams[e["event_type"]].fields}
        assert expected <= e.keys(), f"{e['event_type']} missing {expected - e.keys()}"


def test_freezer_fault_drifts_warm():
    rng = random.Random(7)
    temps = [producer.iot_event(rng, fault_store=1, tick=1000) for _ in range(2000)]
    faulty = [t["value"] for t in temps if t["store_id"] == 1 and t["sensor_type"] == "freezer"]
    healthy = [t["value"] for t in temps if t["store_id"] != 1 and t["sensor_type"] == "freezer"]
    assert sum(faulty) / len(faulty) > -15 > sum(healthy) / len(healthy)
