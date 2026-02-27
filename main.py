"""
TireTrade Pro — FastAPI Backend (v4.0 Web Edition)
===================================================
Tracking is BOOKING-REF based — submits booking number to Terminal49,
then fetches the full shipment + all containers under that booking.

Endpoints:
  GET  /api/health              → Health check (wake-up detection)
  GET  /api/deals               → Load all deals
  POST /api/deals               → Save all deals (full replace)
  GET  /api/deals/export        → Download deals as .json file
  POST /api/deals/import        → Upload and import a .json file
  POST /api/track               → Submit booking ref to Terminal49
  GET  /api/track/{request_id}  → Poll status + get shipment & containers
  GET  /api/track/shipments/search?bol=XXXXX → Search by BOL/booking
"""

import os
import json
import httpx
from pathlib import Path
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "."))
DATA_FILE = DATA_DIR / "tire_deals.json"
TERMINAL49_API_KEY = os.getenv("TERMINAL49_API_KEY", "YOUR_API_KEY_HERE")
T49_BASE = "https://api.terminal49.com/v2"

SCAC_MAP = {
    "Maersk": "MAEU", "MSC": "MSCU", "CMA CGM": "CMDU",
    "Hapag-Lloyd": "HLCU", "COSCO": "COSU", "ONE": "ONEY",
    "Evergreen": "EGLV", "HMM": "HDMU", "ZIM": "ZIMU", "Yang Ming": "YMLU",
}

# ---------------------------------------------------------------------------
# APP SETUP
# ---------------------------------------------------------------------------
app = FastAPI(
    title="TireTrade Pro API",
    version="4.0.0",
    description="Backend for TireTrade Pro — Tire Bale Arbitrage & Logistics",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def read_deals() -> list[dict]:
    if not DATA_FILE.exists():
        return []
    try:
        return json.loads(DATA_FILE.read_text("utf-8")) or []
    except Exception:
        return []


def write_deals(deals: list[dict]) -> dict:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        DATA_FILE.write_text(json.dumps(deals, indent=2, ensure_ascii=False), "utf-8")
        verify = json.loads(DATA_FILE.read_text("utf-8"))
        return {"success": True, "count": len(verify), "path": str(DATA_FILE)}
    except Exception as e:
        return {"success": False, "error": str(e)}


def t49_headers():
    return {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Token {TERMINAL49_API_KEY}",
    }


# ---------------------------------------------------------------------------
# DEAL ENDPOINTS
# ---------------------------------------------------------------------------
@app.get("/api/health")
async def health_check():
    return {
        "status": "ok", "version": "4.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "deals_exist": DATA_FILE.exists(),
    }


@app.get("/api/deals")
async def get_deals():
    deals = read_deals()
    return {"success": True, "deals": deals, "count": len(deals)}


@app.post("/api/deals")
async def save_deals(deals: list[dict]):
    result = write_deals(deals)
    if not result["success"]:
        raise HTTPException(500, result.get("error", "Write failed"))
    return result


@app.get("/api/deals/export")
async def export_deals():
    if not DATA_FILE.exists():
        raise HTTPException(404, "No deals file found")
    return FileResponse(
        str(DATA_FILE), media_type="application/json",
        filename=f"TireTrade_Backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
    )


@app.post("/api/deals/import")
async def import_deals(file: UploadFile = File(...)):
    if not file.filename.endswith(".json"):
        raise HTTPException(400, "Only .json files accepted")
    try:
        data = json.loads(await file.read())
        if not isinstance(data, list):
            raise HTTPException(400, "JSON must be an array of deals")
        result = write_deals(data)
        if not result["success"]:
            raise HTTPException(500, result.get("error"))
        return {"success": True, "count": len(data)}
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON file")


# ---------------------------------------------------------------------------
# TERMINAL49: BOOKING-BASED TRACKING
# ---------------------------------------------------------------------------

class TrackBookingRequest(BaseModel):
    bookingRef: str
    carrier: str  # e.g. "Hapag-Lloyd"


@app.post("/api/track")
async def track_by_booking(req: TrackBookingRequest):
    """
    Submit a tracking request using a BOOKING REFERENCE number.
    Terminal49 accepts booking numbers under request_type="bill_of_lading".
    Returns tracking_request ID so the frontend can poll for shipment + containers.
    """
    if not req.bookingRef or not req.bookingRef.strip():
        raise HTTPException(400, "Booking reference is required")
    if TERMINAL49_API_KEY == "YOUR_API_KEY_HERE":
        raise HTTPException(503, "Terminal49 API key not configured. Set TERMINAL49_API_KEY env var on Render.")

    scac = SCAC_MAP.get(req.carrier)
    if not scac:
        raise HTTPException(400, f'Unknown carrier "{req.carrier}". Supported: {", ".join(SCAC_MAP.keys())}')

    payload = {
        "data": {
            "type": "tracking_request",
            "attributes": {
                "request_type": "bill_of_lading",
                "request_number": req.bookingRef.strip(),
                "scac": scac,
            },
        }
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(f"{T49_BASE}/tracking_requests", json=payload, headers=t49_headers())
            data = resp.json()

            # 422 = already tracked (duplicate)
            if resp.status_code == 422:
                return {
                    "success": True, "status": 422, "duplicate": True, "data": data,
                    "message": "Booking already tracked. Fetching existing shipment data...",
                }

            return {
                "success": resp.status_code in (200, 201, 202),
                "status": resp.status_code, "data": data,
            }
    except httpx.TimeoutException:
        raise HTTPException(504, "Terminal49 request timed out")
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/track/{request_id}")
async def poll_tracking_request(request_id: str):
    """
    Poll a tracking request by ID. When status=succeeded, fetches the full
    shipment object + ALL containers under that booking with full details.
    """
    if TERMINAL49_API_KEY == "YOUR_API_KEY_HERE":
        raise HTTPException(503, "API key not configured")

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            # 1) Get tracking request status
            resp = await client.get(f"{T49_BASE}/tracking_requests/{request_id}", headers=t49_headers())
            if resp.status_code != 200:
                return {"success": False, "status": resp.status_code, "data": resp.json()}

            tr_data = resp.json()
            attrs = tr_data.get("data", {}).get("attributes", {})
            status = attrs.get("status", "unknown")

            if status == "pending":
                return {"success": True, "tracking_status": "pending",
                        "message": "Terminal49 is still fetching data. Poll again in 3-5 seconds."}

            if status == "failed":
                return {"success": False, "tracking_status": "failed",
                        "reason": attrs.get("failed_reason", "Unknown error")}

            # 2) Status is created/succeeded — get the linked shipment
            tracked_obj = (tr_data.get("data", {}).get("relationships", {})
                          .get("tracked_object", {}).get("data"))
            if not tracked_obj or not tracked_obj.get("id"):
                return {"success": True, "tracking_status": status,
                        "message": "Processed but no shipment linked yet. Try again shortly."}

            shipment_id = tracked_obj["id"]

            # 3) Fetch full shipment
            ship_resp = await client.get(f"{T49_BASE}/shipments/{shipment_id}", headers=t49_headers())
            shipment = ship_resp.json().get("data", {}) if ship_resp.status_code == 200 else {}

            # 4) Fetch ALL containers under this shipment
            container_refs = (shipment.get("relationships", {})
                            .get("containers", {}).get("data", []))
            containers = []
            for cref in container_refs:
                cid = cref.get("id")
                if cid:
                    c_resp = await client.get(f"{T49_BASE}/containers/{cid}", headers=t49_headers())
                    if c_resp.status_code == 200:
                        c_data = c_resp.json().get("data", {})
                        c_attrs = c_data.get("attributes", {})
                        containers.append({
                            "id": c_data.get("id"),
                            "number": c_attrs.get("number", ""),
                            "seal_number": c_attrs.get("seal_number", ""),
                            "equipment_type": c_attrs.get("equipment_type", ""),
                            "equipment_length": c_attrs.get("equipment_length", ""),
                            "weight_kg": c_attrs.get("weight_in_lbs"),
                            "status": c_attrs.get("status", ""),
                            "pod_arrived_at": c_attrs.get("pod_arrived_at"),
                            "pod_discharged_at": c_attrs.get("pod_discharged_at"),
                            "pod_full_out_at": c_attrs.get("pod_full_out_at"),
                            "pol_loaded_at": c_attrs.get("pol_loaded_at"),
                            "pol_etd_at": c_attrs.get("pol_etd_at"),
                            "pod_eta_at": c_attrs.get("pod_eta_at"),
                            "pickup_lfd": c_attrs.get("pickup_lfd"),
                            "holds_at_pod_terminal": c_attrs.get("holds_at_pod_terminal"),
                            "fees_at_pod_terminal": c_attrs.get("fees_at_pod_terminal"),
                            "raw": c_attrs,  # full attrs for debugging
                        })

            # 5) Extract key shipment-level fields
            s_attrs = shipment.get("attributes", {})
            shipment_summary = {
                "id": shipment.get("id"),
                "bol_number": s_attrs.get("bill_of_lading_number"),
                "shipping_line": s_attrs.get("shipping_line_name"),
                "scac": s_attrs.get("shipping_line_scac"),
                "vessel": s_attrs.get("pod_vessel_name"),
                "voyage": s_attrs.get("pod_voyage_number"),
                "pol_name": s_attrs.get("port_of_lading_name"),
                "pol_locode": s_attrs.get("port_of_lading_locode"),
                "pod_name": s_attrs.get("port_of_discharge_name"),
                "pod_locode": s_attrs.get("port_of_discharge_locode"),
                "destination": s_attrs.get("destination_name"),
                "pol_etd": s_attrs.get("pol_etd_at"),
                "pol_atd": s_attrs.get("pol_atd_at"),
                "pod_eta": s_attrs.get("pod_eta_at"),
                "pod_ata": s_attrs.get("pod_ata_at"),
                "status": s_attrs.get("status"),
            }

            return {
                "success": True,
                "tracking_status": "succeeded",
                "shipment": shipment_summary,
                "containers": containers,
                "containerCount": len(containers),
            }

    except httpx.TimeoutException:
        raise HTTPException(504, "Timeout polling Terminal49")
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/track/shipments/search")
async def search_shipments_by_bol(bol: str = Query(..., description="BOL or Booking number")):
    """
    Search existing tracked shipments by BOL/booking number.
    Useful when booking was already submitted previously.
    """
    if TERMINAL49_API_KEY == "YOUR_API_KEY_HERE":
        raise HTTPException(503, "API key not configured")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{T49_BASE}/shipments",
                params={"filter[bill_of_lading_number]": bol.strip()},
                headers=t49_headers(),
            )
            data = resp.json()
            shipments = data.get("data", [])

            results = []
            for ship in shipments:
                container_refs = ship.get("relationships", {}).get("containers", {}).get("data", [])
                containers = []
                for cref in container_refs:
                    cid = cref.get("id")
                    if cid:
                        c_resp = await client.get(f"{T49_BASE}/containers/{cid}", headers=t49_headers())
                        if c_resp.status_code == 200:
                            containers.append(c_resp.json().get("data", {}))

                s_attrs = ship.get("attributes", {})
                results.append({
                    "shipment": {
                        "id": ship.get("id"),
                        "bol_number": s_attrs.get("bill_of_lading_number"),
                        "vessel": s_attrs.get("pod_vessel_name"),
                        "pol_name": s_attrs.get("port_of_lading_name"),
                        "pod_name": s_attrs.get("port_of_discharge_name"),
                        "pod_eta": s_attrs.get("pod_eta_at"),
                        "status": s_attrs.get("status"),
                    },
                    "containers": containers,
                    "containerCount": len(containers),
                })

            return {"success": True, "results": results, "totalShipments": len(results)}
    except Exception as e:
        raise HTTPException(502, str(e))


# ---------------------------------------------------------------------------
# STARTUP
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup_seed():
    if not DATA_FILE.exists():
        print(f"[startup] Creating empty data file at {DATA_FILE}")
        write_deals([])
    else:
        print(f"[startup] Loaded {len(read_deals())} deals from {DATA_FILE}")
