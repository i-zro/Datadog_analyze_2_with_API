from __future__ import annotations
from typing import Any, Dict, List, Optional
import os
import json
import re
from datetime import datetime

import requests
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field

# -----------------------------------------------------------
# 설정 & 상수
# -----------------------------------------------------------
DD_API_KEY = os.getenv("DD_API_KEY", "")
DD_APP_KEY = os.getenv("DD_APP_KEY", "")
DD_SITE = os.getenv("DD_SITE", "datadoghq.com")

DEFAULT_HIDDEN_COLUMNS = [
    "session.id","usr.id","attribute.os.build",
    "type","application.id","session.type","view.url","view.referrer",
    "usr.name","usr.email","action.type","action.target.name",
    "resource.type","resource.url","error.message","error.source","error.stack",
    "device.type","os.name","browser.name",
    "attributes.os.build","attributes.os.name","attributes.os.version_major",
    "attributes.resource.size","attributes.resource.method",
    "attributes.resource.provider.domain","attributes.resource.provider.name","attributes.resource.provider.type",
    "attributes.resource.type","attributes.resource.id","attributes.resource.url",
    "attributes.resource.url_host","attributes.resource.url_scheme","attributes.resource.url_path_group",
    "attributes.session.matching_retention_filter.name","attributes.session.matching_retention_filter.id",
    "attributes.session.type","attributes.session.plan","attributes.type",
    "attributes.geo.continent","attributes.geo.country","attributes.geo.as.domain","attributes.geo.as.name","attributes.geo.as.type",
    "attributes.geo.country_iso_code","attributes.geo.city","attributes.geo.latitude","attributes.geo.continent_code",
    "attributes.geo.subdivision_iso_code","attributes.geo.country_subdivision_iso_code","attributes.geo.location",
    "attributes.geo.country_subdivision","attributes.geo.longitude",
    "attributes.view.url_path_group","attributes.view.id","attributes.view.url","attributes.view.url_path",
    "attributes.application.name","attributes.application.short_name","attributes.application.id",
    "attributes.connectivity.cellular.carrier_name","attributes.connectivity.status",
    "attributes.usr.anonymous_id","attributes.usr.usr_id","attributes.service",
    "attributes.device.name",
    "attributes.device.model",
    "attributes.device.type",
    "attributes.device.brand",
    "attributes.device.architecture",
    "attributes._dd.format_version",
    "tags",
    "attributes.usr.id",
    "timestamp",
]

# -----------------------------------------------------------
# 유틸 함수
# -----------------------------------------------------------

def iso_to_kst_ms(iso_str: str, tz_name: str = "Asia/Seoul") -> str:
    if not iso_str:
        return ""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    try:
        import pytz
        kst = pytz.timezone(tz_name)
        k = dt.astimezone(kst)
    except Exception:
        from datetime import timezone, timedelta
        k = dt.astimezone(timezone(timedelta(hours=9)))
    return k.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(k.strftime('%f'))//1000:03d} KST"


def flatten(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            flatten(f"{prefix}.{k}" if prefix else k, v, out)
    elif isinstance(obj, list):
        out[prefix] = ", ".join(map(str, obj))
    else:
        out[prefix] = obj


def build_rows_dynamic(all_events: List[Dict[str, Any]], tz_name: str = "Asia/Seoul") -> List[Dict[str, Any]]:
    processed_rows: List[Dict[str, Any]] = []
    for event in all_events:
        attrs = event.get("attributes", {}) or {}
        flat_row: Dict[str, Any] = {}
        flatten("", attrs, flat_row)

        usr_info = event.get("usr") or attrs.get("usr")
        if usr_info:
            flatten("usr", usr_info, flat_row)
        tags = event.get("tags") or attrs.get("tags")
        if tags:
            flat_row["tags"] = tags

        flat_row["timestamp(KST)"] = iso_to_kst_ms(attrs.get("timestamp"), tz_name)

        call_id_val = (
            flat_row.get("attributes.context.callID")
            or flat_row.get("attributes.context.callId")
        )
        if call_id_val is not None:
            flat_row["Call ID"] = call_id_val
            flat_row.pop("attributes.context.callID", None)
            flat_row.pop("attributes.context.callId", None)

        processed_rows.append(flat_row)
    return processed_rows


def summarize_calls(flat_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    from collections import defaultdict
    calls = defaultdict(list)
    for row in flat_rows:
        cid = row.get("Call ID")
        if cid:
            calls[cid].append(row)

    summaries: List[Dict[str, Any]] = []
    for call_id, events in calls.items():
        termination_reason = None
        bye_reason = None
        send_packets: List[Any] = []
        receive_health_check: List[Any] = []
        request_status = accept_status = reject_status = end_status = None
        active_ts = stopping_ts = None

        overall_end_time_str = events[0].get("timestamp(KST)")
        overall_start_time_str = events[-1].get("timestamp(KST)")

        for event in events:
            path = event.get("attributes.resource.url_path")
            status_code = event.get("attributes.resource.status_code")
            timestamp_str = event.get("timestamp(KST)")

            if event.get("attributes.context.method") == "BYE":
                bye_reason = event.get("attributes.context.reason")

            if path == "/res/SDK_CALL_STATUS_ACTIVE" and active_ts is None:
                active_ts = timestamp_str
            elif path == "/res/SDK_CALL_STATUS_STOPPING":
                if stopping_ts is None:
                    stopping_ts = timestamp_str
                if termination_reason is None:
                    event_type = event.get("attributes.context.eventType")
                    event_detail = event.get("attributes.context.eventDetail")
                    parts = []
                    if event_type:
                        parts.append(str(event_type))
                    if event_detail:
                        parts.append(f"({event_detail})")
                    if parts:
                        termination_reason = " ".join(parts)

            elif path == "/res/requestVoiceCall" and request_status is None:
                request_status = status_code
            elif path == "/res/acceptCall" and accept_status is None:
                accept_status = status_code
            elif path == "/res/rejectCall" and reject_status is None:
                reject_status = status_code
            elif path == "/res/endCall" and end_status is None:
                end_status = status_code
            elif path == "/res/ENGINE_SendPackets" and len(send_packets) < 3:
                count = event.get("attributes.context.totalCount")
                if count is not None:
                    send_packets.append(count)
            elif path == "/res/ENGINE_ReceiveHealthCheck" and len(receive_health_check) < 3:
                count = event.get("attributes.context.totalCount")
                if count is not None:
                    receive_health_check.append(count)

        def parse_kst(ts: Optional[str]) -> Optional[datetime]:
            if not ts:
                return None
            try:
                return datetime.strptime(ts.replace(" KST", ""), "%Y-%m-%d %H:%M:%S.%f")
            except Exception:
                return None

        duration_str = ""
        stop_dt, active_dt = parse_kst(stopping_ts), parse_kst(active_ts)
        if stop_dt and active_dt:
            duration_seconds = (stop_dt - active_dt).total_seconds()
            duration_str = f"{duration_seconds:.1f} 초"
        elif not active_ts:
            duration_str = "ACTIVE 없음"
        elif not stopping_ts:
            duration_str = "STOPPING 없음"
        else:
            duration_str = "시간 포맷 오류"

        summaries.append({
            "Call ID": call_id,
            "Start Time (KST)": overall_start_time_str,
            "End Time (KST)": overall_end_time_str,
            "Duration": duration_str,
            "종료 사유": termination_reason,
            "BYE reason": bye_reason,
            "requestVoiceCall_status_code": request_status,
            "acceptCall_status_code": accept_status,
            "rejectCall_status_code": reject_status,
            "endCall_status_code": end_status,
            "SendPackets 수": send_packets,
            "ReceiveHealthCheck 수": receive_health_check,
        })
    return summaries


def analyze_rtp_timeouts(flat_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    from collections import defaultdict
    rtp_timeout_call_ids = set()
    for row in flat_rows:
        reason = row.get("attributes.context.reason", "")
        if isinstance(reason, str) and "rtp" in reason.lower():
            cid = row.get("Call ID")
            if cid:
                rtp_timeout_call_ids.add(cid)

    if not rtp_timeout_call_ids:
        return []

    calls = defaultdict(list)
    for row in flat_rows:
        cid = row.get("Call ID")
        if cid in rtp_timeout_call_ids:
            calls[cid].append(row)

    results: List[Dict[str, Any]] = []
    for call_id, events in calls.items():
        active_ts = stopping_ts = None
        bye_method_source = "N/A"
        rtp_timeout_reason = "N/A"
        usr_id = "N/A"
        first_version = "N/A"

        if events:
            tags_str = events[0].get("tags", "")
            if isinstance(tags_str, str):
                m = re.search(r"first_version:([^,]+)", tags_str)
                if m:
                    first_version = m.group(1).strip()
            for e in events:
                if e.get("attributes.usr.id"):
                    usr_id = e.get("attributes.usr.id")
                    break

        overall_end_time_str = events[0].get("timestamp(KST)")
        overall_start_time_str = events[-1].get("timestamp(KST)")

        for e in events:
            path = e.get("attributes.resource.url_path", "")
            ts = e.get("timestamp(KST)")
            reason = e.get("attributes.context.reason", "")
            if isinstance(reason, str) and "rtp" in reason.lower() and rtp_timeout_reason == "N/A":
                rtp_timeout_reason = reason
            if path == "/res/SDK_CALL_STATUS_ACTIVE" and active_ts is None:
                active_ts = ts
            elif path == "/res/SDK_CALL_STATUS_STOPPING" and stopping_ts is None:
                stopping_ts = ts
            if e.get("attributes.context.method") == "BYE":
                lp = path.lower()
                if "longres" in lp:
                    bye_method_source = "longRes"
                elif "restreq" in lp:
                    bye_method_source = "restReq"
                elif "sendmessage" in lp:
                    bye_method_source = "sendMessage"
                elif "recvmessage" in lp:
                    bye_method_source = "recvMessage"
                else:
                    bye_method_source = "Unknown"

        def parse_kst(ts: Optional[str]) -> Optional[datetime]:
            if not ts:
                return None
            try:
                return datetime.strptime(ts.replace(" KST", ""), "%Y-%m-%d %H:%M:%S.%f")
            except Exception:
                return None

        duration_str = ""
        stop_dt, active_dt = parse_kst(stopping_ts), parse_kst(active_ts)
        if stop_dt and active_dt:
            duration_seconds = (stop_dt - active_dt).total_seconds()
            duration_str = f"{duration_seconds:.1f} 초"
        elif not active_ts:
            duration_str = "ACTIVE 없음"
        elif not stopping_ts:
            duration_str = "STOPPING 없음"
        else:
            duration_str = "시간 포맷 오류"

        results.append({
            "Call ID": call_id,
            "App Version": first_version,
            "Start Time (KST)": overall_start_time_str,
            "End Time (KST)": overall_end_time_str,
            "통화 시간": duration_str,
            "BYE Reason": rtp_timeout_reason,
            "BYE 전달": bye_method_source,
            "usr.id": usr_id,
        })

    return sorted(results, key=lambda x: x.get("Start Time (KST)", ""), reverse=True)

# -----------------------------------------------------------
# Datadog API 클라이언트
# -----------------------------------------------------------

class DatadogAPIClient:
    def __init__(self, api_key: str, app_key: str, site: str = "datadoghq.com"):
        if not (api_key and app_key):
            raise ValueError("Datadog API_KEY와 APP_KEY는 필수입니다.")
        self.api_key = api_key
        self.app_key = app_key
        self.site = site
        self.base_url = f"https://api.{self.site}"
        self.session = requests.Session()

    @property
    def _headers_v2_json(self) -> Dict[str, str]:
        return {
            "DD-API-KEY": self.api_key,
            "DD-APPLICATION-KEY": self.app_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        try:
            r = self.session.post(url, headers=self._headers_v2_json, json=body, timeout=30)
            try:
                payload = r.json()
            except (ValueError, json.JSONDecodeError):
                payload = {"raw_text": r.text}
            r.raise_for_status()
            return payload
        except requests.exceptions.HTTPError as e:
            detail = getattr(e.response, "text", str(e))
            raise HTTPException(status_code=e.response.status_code if e.response else 502, detail=detail)
        except requests.exceptions.RequestException as e:
            raise HTTPException(status_code=502, detail=f"Upstream request failed: {e}")


# -----------------------------------------------------------
# 스키마 (Pydantic)
# -----------------------------------------------------------

class TimeRange(BaseModel):
    from_ts: str = Field(..., description="ISO8601 (UTC) e.g. 2025-09-13T00:00:00Z")
    to_ts: str = Field(..., description="ISO8601 (UTC) e.g. 2025-09-13T01:00:00Z")

class SearchRequest(TimeRange):
    query: str = Field("*", description="Datadog RUM 검색 쿼리")
    limit_per_page: int = Field(200, ge=1, le=1000)
    max_pages: int = Field(5, ge=1, le=50)

class SearchResponse(BaseModel):
    count: int
    data: List[Dict[str, Any]]
    meta: Dict[str, Any] = {}

class SummarizeRequest(SearchRequest):
    pass

class CallSummary(BaseModel):
    Call_ID: Optional[str] = Field(None, alias="Call ID")
    Start_Time_KST: Optional[str] = Field(None, alias="Start Time (KST)")
    End_Time_KST: Optional[str] = Field(None, alias="End Time (KST)")
    Duration: Optional[str]
    종료_사유: Optional[str] = Field(None, alias="종료 사유")
    BYE_reason: Optional[str] = Field(None, alias="BYE reason")
    requestVoiceCall_status_code: Optional[int]
    acceptCall_status_code: Optional[int]
    rejectCall_status_code: Optional[int]
    endCall_status_code: Optional[int]
    SendPackets_수: Optional[Any] = Field(None, alias="SendPackets 수")
    ReceiveHealthCheck_수: Optional[Any] = Field(None, alias="ReceiveHealthCheck 수")

class SummarizeResponse(BaseModel):
    total_events: int
    total_calls: int
    summaries: List[CallSummary]

class RTPAnalysisRequest(SearchRequest):
    pass

class RTPItem(BaseModel):
    Call_ID: Optional[str] = Field(None, alias="Call ID")
    App_Version: Optional[str] = Field(None, alias="App Version")
    Start_Time_KST: Optional[str] = Field(None, alias="Start Time (KST)")
    End_Time_KST: Optional[str] = Field(None, alias="End Time (KST)")
    통화_시간: Optional[str] = Field(None, alias="통화 시간")
    BYE_Reason: Optional[str] = Field(None, alias="BYE Reason")
    BYE_전달: Optional[str] = Field(None, alias="BYE 전달")
    usr_id: Optional[str] = Field(None, alias="usr.id")

class RTPAnalysisResponse(BaseModel):
    total_related_events: int
    calls: List[RTPItem]


# -----------------------------------------------------------
# FastAPI 앱 (지연 생성 의존성)
# -----------------------------------------------------------
app = FastAPI(title="Datadog RUM Backend", version="1.0.0")

_client_singleton: Optional[DatadogAPIClient] = None

def get_client() -> DatadogAPIClient:
    global _client_singleton
    if _client_singleton is None:
        if not (DD_API_KEY and DD_APP_KEY):
            raise HTTPException(status_code=500, detail="Datadog keys not configured")
        _client_singleton = DatadogAPIClient(DD_API_KEY, DD_APP_KEY, DD_SITE)
    return _client_singleton


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


def _rum_search(req: SearchRequest, client: DatadogAPIClient) -> Dict[str, Any]:
    body = {
        "filter": {"from": req.from_ts, "to": req.to_ts, "query": req.query},
        "page": {"limit": req.limit_per_page},
        "sort": "-timestamp",
    }
    all_events: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    for _ in range(req.max_pages):
        if cursor:
            body["page"]["cursor"] = cursor
        payload = client.post("/api/v2/rum/events/search", body)
        events = payload.get("data", [])
        all_events.extend(events)
        cursor = payload.get("meta", {}).get("page", {}).get("after")
        if not cursor:
            break
    return {"data": all_events, "meta": {"pages": _, "cursor": cursor}}


@app.post("/rum/search", response_model=SearchResponse)
def rum_search(req: SearchRequest, client: DatadogAPIClient = Depends(get_client)):
    out = _rum_search(req, client)
    return {"count": len(out["data"]), "data": out["data"], "meta": out.get("meta", {})}


@app.post("/rum/summarize", response_model=SummarizeResponse)
def rum_summarize(req: SummarizeRequest, client: DatadogAPIClient = Depends(get_client)):
    out = _rum_search(req, client)
    events = out["data"]
    flat = build_rows_dynamic(events, tz_name="Asia/Seoul")
    sums = summarize_calls(flat)
    return {
        "total_events": len(events),
        "total_calls": len(sums),
        "summaries": sums,
    }


@app.post("/rum/rtp-analysis", response_model=RTPAnalysisResponse)
def rum_rtp_analysis(req: RTPAnalysisRequest, client: DatadogAPIClient = Depends(get_client)):
    rtp_reason_query = "@context.reason:(*RTP* OR *rtp*)"
    step1_req = SearchRequest(
        query=rtp_reason_query,
        from_ts=req.from_ts,
        to_ts=req.to_ts,
        limit_per_page=min(req.limit_per_page, 1000),
        max_pages=req.max_pages,
    )
    step1 = _rum_search(step1_req, client)
    flat_rtp = build_rows_dynamic(step1["data"], tz_name="Asia/Seoul")

    call_ids = sorted({row.get("Call ID") for row in flat_rtp if row.get("Call ID")})
    if not call_ids:
        return {"total_related_events": 0, "calls": []}

    call_id_query_part = " OR ".join(f'"{cid}"' for cid in call_ids)
    full_query = f'(@context.callID:({call_id_query_part}) OR @context.callId:({call_id_query_part}))'
    step2_req = SearchRequest(
        query=full_query,
        from_ts=req.from_ts,
        to_ts=req.to_ts,
        limit_per_page=min(req.limit_per_page, 1000),
        max_pages=req.max_pages,
    )
    step2 = _rum_search(step2_req, client)
    flat_all = build_rows_dynamic(step2["data"], tz_name="Asia/Seoul")
    analysis = analyze_rtp_timeouts(flat_all)

    return {
        "total_related_events": len(flat_all),
        "calls": analysis,
    }


@app.get("/rum/columns/hidden")
def get_default_hidden_columns_api() -> Dict[str, List[str]]:
    return {"hidden_columns": list(DEFAULT_HIDDEN_COLUMNS)}


# -----------------------------------------------------------
# 로컬 실행
#   uvicorn fastapi_app.app:app --reload --port 8080
# -----------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fastapi_app.app:app", host="0.0.0.0", port=8080, reload=True)