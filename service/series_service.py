from datetime import datetime, timedelta, timezone
from typing import Optional

from service.matriz_requester import MatrizRequester


# Mapping: resolution -> (base_resolution, grouping_minutes_or_method)
RESOLUTION_MAP = {
    "1":  ("1", None),
    "5":  ("1", 5),
    "15": ("1", 15),
    "30": ("1", 30),
    "1H": ("1", 60),
    "4H": ("1", 240),
    "D":  ("D", None),
    "S":  ("D", "week"),
    "M":  ("D", "month"),
}

# Max history for 1-min base resolution
MAX_1MIN_DAYS = 5


class SeriesService:

    def __init__(self, requester: MatrizRequester):
        self.requester = requester
        # Cache: {(instrument, base_resolution): [candles sorted by d]}
        self._cache: dict[tuple[str, str], list[dict]] = {}

    def get_series(self, instrument: str, resolution: str,
                   from_dt: datetime, to_dt: datetime) -> dict:
        if resolution not in RESOLUTION_MAP:
            return {"noData": True, "series": [], "error": f"Resolucion invalida: {resolution}"}

        base_res, grouping = RESOLUTION_MAP[resolution]

        # Clamp from_dt for 1-min resolution
        if base_res == "1":
            min_from = datetime.now(timezone.utc) - timedelta(days=MAX_1MIN_DAYS)
            if from_dt < min_from:
                from_dt = min_from

        # Fetch/update cache
        candles = self._get_base_candles(instrument, base_res, from_dt, to_dt)

        # Filter to requested range
        from_iso = from_dt.isoformat()
        to_iso = to_dt.isoformat()
        filtered = [c for c in candles if from_iso <= c["d"] <= to_iso]

        # Aggregate if needed
        if grouping is not None:
            filtered = self._aggregate(filtered, resolution, grouping)

        return {"noData": len(filtered) == 0, "series": filtered}

    def _get_base_candles(self, instrument: str, base_res: str,
                          from_dt: datetime, to_dt: datetime) -> list[dict]:
        cache_key = (instrument, base_res)
        cached = self._cache.get(cache_key, [])

        from_iso = from_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        to_iso = to_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        if cached:
            cache_min = cached[0]["d"]
            cache_max = cached[-1]["d"]

            # Always re-fetch the last candle (may be incomplete) + any missing ranges
            ranges_to_fetch = []

            # Need data before cache?
            if from_iso < cache_min:
                ranges_to_fetch.append((from_iso, cache_min))

            # Need data after cache (or refresh last candle)?
            # Re-fetch from the last cached candle onward
            if to_iso >= cache_max:
                ranges_to_fetch.append((cache_max, to_iso))

            if not ranges_to_fetch:
                return cached

            for r_from, r_to in ranges_to_fetch:
                new_candles = self._fetch_from_broker(instrument, base_res, r_from, r_to)
                if new_candles:
                    cached = self._merge_candles(cached, new_candles)

            self._cache[cache_key] = cached
        else:
            # No cache at all, fetch everything
            new_candles = self._fetch_from_broker(instrument, base_res, from_iso, to_iso)
            if new_candles:
                cached = new_candles
                self._cache[cache_key] = cached

        return cached

    def _fetch_from_broker(self, instrument: str, resolution: str,
                           from_iso: str, to_iso: str) -> list[dict]:
        resource = f"api/v2/series/securities/{instrument}?resolution={resolution}&from={from_iso}&to={to_iso}"
        print(f"[SeriesService] Fetching: {resource}", flush=True)
        try:
            response = self.requester.http_get(resource)
        except Exception as e:
            print(f"[SeriesService] Error fetching series: {e}", flush=True)
            return []

        if not response:
            return []

        series = response.get("series", [])
        if not series:
            return []

        # Sort by d
        series.sort(key=lambda c: c["d"])
        return series

    def _merge_candles(self, existing: list[dict], new: list[dict]) -> list[dict]:
        # Deduplicate by d, preferring new data (more recent fetch)
        by_d = {c["d"]: c for c in existing}
        for c in new:
            by_d[c["d"]] = c
        merged = sorted(by_d.values(), key=lambda c: c["d"])
        return merged

    def _aggregate(self, candles: list[dict], resolution: str,
                   grouping) -> list[dict]:
        if not candles:
            return []

        if isinstance(grouping, int):
            return self._aggregate_by_count(candles, grouping, resolution)
        elif grouping == "week":
            return self._aggregate_by_week(candles, resolution)
        elif grouping == "month":
            return self._aggregate_by_month(candles, resolution)
        return candles

    def _aggregate_by_count(self, candles: list[dict], count: int,
                            resolution: str) -> list[dict]:
        result = []
        for i in range(0, len(candles), count):
            group = candles[i:i + count]
            result.append(self._build_candle(group, resolution))
        return result

    def _aggregate_by_week(self, candles: list[dict], resolution: str) -> list[dict]:
        groups: dict[str, list[dict]] = {}
        for c in candles:
            dt = datetime.fromisoformat(c["d"].replace("Z", "+00:00"))
            # ISO week start (Monday)
            week_start = dt - timedelta(days=dt.weekday())
            key = week_start.strftime("%Y-%m-%d")
            groups.setdefault(key, []).append(c)

        result = []
        for key in sorted(groups.keys()):
            result.append(self._build_candle(groups[key], resolution))
        return result

    def _aggregate_by_month(self, candles: list[dict], resolution: str) -> list[dict]:
        groups: dict[str, list[dict]] = {}
        for c in candles:
            dt = datetime.fromisoformat(c["d"].replace("Z", "+00:00"))
            key = dt.strftime("%Y-%m")
            groups.setdefault(key, []).append(c)

        result = []
        for key in sorted(groups.keys()):
            result.append(self._build_candle(groups[key], resolution))
        return result

    def _build_candle(self, group: list[dict], resolution: str) -> dict:
        return {
            "o": group[0]["o"],
            "h": max(c["h"] for c in group),
            "l": min(c["l"] for c in group),
            "c": group[-1]["c"],
            "v": sum(c.get("v", 0) for c in group),
            "d": group[0]["d"],
            "r": resolution,
            "sid": group[0].get("sid", ""),
        }
