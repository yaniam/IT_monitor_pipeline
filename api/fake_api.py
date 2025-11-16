from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from common.segment_utils import (
    DATASET_PATH,
    build_payload,
    filter_recent,
    load_devices,
    maybe_update_last_connections,
    parse_current_time,
    write_devices,
)

app = FastAPI(title="Synthetic Segment Stats API", version="1.0.0")


class SegmentRequest(BaseModel):
    current_time: str | None = Field(
        default=None, description="Override current time (YYYY-MM-DD HH:MM:SS)."
    )
    window_minutes: int = Field(
        default=60, ge=1, le=1440, description="Lookback window size in minutes."
    )
    frequency_minutes: int = Field(
        default=15, ge=1, le=1440, description="Metadata for downstream scheduling."
    )
    update_probability: float = Field(
        default=0.10, ge=0.0, le=1.0, description="Chance a healthy machine refreshes its timestamp."
    )


@app.get("/health", tags=["system"])
def health_check() -> dict:
    return {"status": "ok"}


@app.post("/segment-stats", tags=["segments"])
def segment_stats(request: SegmentRequest) -> dict:
    try:
        now = parse_current_time(request.current_time)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        devices = load_devices()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    maybe_update_last_connections(devices, now, request.update_probability)
    write_devices(DATASET_PATH, devices)

    recent_devices = filter_recent(devices, now, request.window_minutes)
    payload = build_payload(recent_devices, now, request.window_minutes, request.frequency_minutes)
    return payload

