"""Poll the USGS earthquake feed and publish each event to JetStream.

Re-fetches overlap; JetStream drops repeats via Nats-Msg-Id (id + updated) within duplicate_window.
"""
import asyncio, json, os, urllib.request

import nats
from nats.js.api import StreamConfig

FEED = os.environ.get("FEED", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson")
EVERY = int(os.environ.get("EVERY_S", "60"))


def fetch():
    req = urllib.request.Request(FEED, headers={"User-Agent": "sail-lake-demo"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["features"]


def to_event(f):
    p, (lon, lat, depth) = f["properties"], f["geometry"]["coordinates"]
    return {"id": f["id"], "time": p["time"], "updated": p["updated"], "mag": p["mag"], "magType": p["magType"],
            "place": p["place"], "type": p["type"], "tsunami": p["tsunami"], "lon": lon, "lat": lat, "depth": depth}


async def main():
    nc = await nats.connect(os.environ.get("NATS_URL", "nats://nats:4222"))
    js = nc.jetstream()
    # all_day items live 24h in the feed, 25h window covers overlap
    await js.add_stream(StreamConfig(name="QUAKES", subjects=["quakes.>"], duplicate_window=90000, max_age=7 * 86400))
    while True:
        try:
            feats = await asyncio.to_thread(fetch)
            new = 0
            for f in feats:
                e = to_event(f)
                ack = await js.publish("quakes.raw", json.dumps(e).encode(), headers={"Nats-Msg-Id": f"{e['id']}:{e['updated']}"})
                new += not ack.duplicate
            print(f"fetched={len(feats)} new={new}", flush=True)
        except Exception as ex:  # keep polling through feed/network blips
            print(f"error: {ex!r}", flush=True)
        await asyncio.sleep(EVERY)


if __name__ == "__main__":
    asyncio.run(main())
