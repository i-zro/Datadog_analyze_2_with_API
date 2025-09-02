import json
import requests
from typing import List, Tuple, Dict, Any
import streamlit as st

from .config import get_search_url, Settings
from .transform import iso_to_kst_ms

def build_usr_query(usr_id_value: str) -> str:
    """usr.id 값에 따라 Datadog 검색 쿼리를 생성합니다."""
    if not usr_id_value:
        # usr.id가 비어있으면 모든 사용자를 대상으로 합니다.
        return "*"
    # 쿼리 내 큰따옴표를 이스케이프 처리하여 안전하게 만듭니다.
    safe = usr_id_value.replace('"', '\\"')
    return f'@usr.id:"{safe}"'

def search_rum_events_usr_id(
    settings: Settings,
    usr_id_value: str,
    from_ts: str,
    to_ts: str,
    limit_per_page: int = 200,
    max_pages: int = 5,
) -> List[Dict[str, Any]]:
    """
    Datadog RUM 이벤트 검색 API를 호출하여 지정된 조건에 맞는 이벤트를 가져옵니다.

    - 기간: from_ts ~ to_ts (UTC, ISO 8601 형식)
    - 필터: @usr.id:"..." (비어있으면 전체)
    - 페이지네이션: 커서를 따라가며 max_pages까지 수집

    Args:
        settings: API 키, 앱 키, 사이트 정보가 담긴 설정 객체
        usr_id_value: 검색할 사용자 ID
        from_ts: 검색 시작 시간 (UTC, ISO 8601)
        to_ts: 검색 종료 시간 (UTC, ISO 8601)
        limit_per_page: 페이지당 가져올 이벤트 수
        max_pages: 최대 페이지 수

    Returns:
        검색된 RUM 이벤트의 원본(raw) 목록
    """
    search_url = get_search_url(settings.site)

    # API 요청 본문 구성
    body = {
        "filter": {"from": from_ts, "to": to_ts, "query": build_usr_query(usr_id_value)},
        "page": {"limit": limit_per_page},
        "sort": "-timestamp",  # 최신순으로 정렬
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DD-API-KEY": settings.api_key,
        "DD-APPLICATION-KEY": settings.app_key,
    }

    all_events: List[Dict[str, Any]] = []
    cursor = None

    # 페이지네이션을 통해 여러 페이지의 결과를 가져옵니다.
    for _ in range(max_pages):
        if cursor:
            body["page"]["cursor"] = cursor

        try:
            resp = requests.post(search_url, headers=headers, data=json.dumps(body), timeout=30)
            resp.raise_for_status()  # HTTP 오류 발생 시 예외 발생
        except requests.exceptions.RequestException as e:
            st.error(f"API 요청 실패: {e}")
            try:
                st.code(resp.text, language="json")
            except Exception:
                st.write(resp.text)
            return []

        data = resp.json()
        events = data.get("data", [])
        all_events.extend(events)

        # 다음 페이지를 위한 커서 값 추출
        cursor = data.get("meta", {}).get("page", {}).get("after")
        if not cursor:
            # 다음 페이지가 없으면 종료
            break

    return all_events
