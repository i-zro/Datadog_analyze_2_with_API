import json
import requests
from typing import List, Tuple, Dict, Any
import streamlit as st

from .config import get_search_url, Settings
from .transform import iso_to_kst_ms

def build_usr_query(usr_id_value: str) -> str:
    if not usr_id_value:
        return "*"
    safe = usr_id_value.replace('"', '\\"')
    return f'@usr.id:"{safe}"'

def search_rum_events_usr_id(
    settings: Settings,
    usr_id_value: str,
    from_ts: str,
    to_ts: str,
    limit_per_page: int = 200,
    max_pages: int = 5,
    tz_name: str = "Asia/Seoul",
) -> Tuple[list, List[Dict[str, Any]]]:
    """
    Datadog RUM 이벤트 검색:
    - 기간: from_ts ~ to_ts
    - 필터: @usr.id:"..." (비어있으면 전체)
    - 페이지네이션: 커서 따라가며 max_pages까지 수집
    """
    search_url = get_search_url(settings.site)

    body = {
        "filter": {"from": from_ts, "to": to_ts, "query": build_usr_query(usr_id_value)},
        "page": {"limit": limit_per_page},
        "sort": "-timestamp",
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DD-API-KEY": settings.api_key,
        "DD-APPLICATION-KEY": settings.app_key,
    }

    all_events: List[Dict[str, Any]] = []
    cursor = None

    for _ in range(max_pages):
        if cursor:
            body["page"]["cursor"] = cursor

        resp = requests.post(search_url, headers=headers, data=json.dumps(body), timeout=30)
        if resp.status_code != 200:
            st.error(f"HTTP {resp.status_code} {resp.reason}")
            try:
                st.code(resp.text, language="json")
            except Exception:
                st.write(resp.text)
            return [], []

        data = resp.json()
        events = data.get("data", [])
        all_events.extend(events)

        cursor = data.get("meta", {}).get("page", {}).get("after")
        if not cursor:
            break

    # 간단 행(호환성 유지용)
    def _row(e):
        attrs = e.get("attributes", {}) or {}
        kst_str = iso_to_kst_ms(attrs.get("timestamp"), tz_name)
        return {
            "timestamp(KST)": kst_str,
            "type": attrs.get("type"),
            "service": attrs.get("service"),
            "view.url": (attrs.get("view", {}) or {}).get("url") if isinstance(attrs.get("view"), dict) else None,
            "session.id": (attrs.get("session", {}) or {}).get("id") if isinstance(attrs.get("session"), dict) else None,
            "usr.id": (attrs.get("usr", {}) or {}).get("id") if isinstance(attrs.get("usr"), dict) else None,
            "action.target.name": ((attrs.get("action", {}) or {}).get("target", {}) or {}).get("name") if isinstance(attrs.get("action"), dict) else None,
            "error.message": (attrs.get("error", {}) or {}).get("message") if isinstance(attrs.get("error"), dict) else None,
        }

    rows = [_row(e) for e in all_events]
    return rows, all_events
