# streamlit_app.py
import os
import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime
from dateutil import tz

# =======================
# 환경/비밀키 로드
# =======================
DD_API_KEY = st.secrets.get("DD_API_KEY", os.getenv("DD_API_KEY"))
DD_APP_KEY = st.secrets.get("DD_APP_KEY", os.getenv("DD_APP_KEY"))
DD_SITE = st.secrets.get("DD_SITE", os.getenv("DD_SITE", "datadoghq.com"))  # 예: ap1.datadoghq.com, datadoghq.com, eu.datadoghq.com

API_BASE = f"https://api.{DD_SITE}"
SEARCH_URL = f"{API_BASE}/api/v2/rum/events/search"

st.set_page_config(page_title="Datadog RUM Search", layout="wide")
st.title("Datadog RUM 검색 (usr.id 기준, 최근 N분 / KST ms 표시)")

# 세션 상태 초기화
if "df_base" not in st.session_state:
    st.session_state.df_base = None
if "hide_defaults" not in st.session_state:
    st.session_state.hide_defaults = ["session.id", "usr.id", "attribute.os.build"]

# 키 검증
if not (DD_API_KEY and DD_APP_KEY):
    st.error("DD_API_KEY / DD_APP_KEY가 설정되어 있지 않습니다. .streamlit/secrets.toml 또는 환경변수를 확인하세요.")
    st.stop()

# =======================
# 유틸 함수
# =======================
def build_usr_query(usr_id_value: str) -> str:
    """Datadog RUM 검색 쿼리 생성: @usr.id:"<값>" (빈 값이면 전체 *)"""
    if not usr_id_value:
        return "*"
    safe = usr_id_value.replace('"', '\\"')
    return f'@usr.id:"{safe}"'

def iso_to_kst_ms(iso_str: str, tz_name: str = "Asia/Seoul") -> str:
    """ISO8601(Z) → KST 변환 + 밀리초 표기 (예: 2025-08-25 11:12:13.456 KST)"""
    if not iso_str:
        return ""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    kst = tz.gettz(tz_name)
    k = dt.astimezone(kst)
    return k.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(k.strftime('%f'))//1000:03d} KST"

def flatten(prefix, obj, out):
    """중첩 dict/list 평탄화: a.b.c 형태 키 생성, 리스트는 최대 10개 join"""
    if isinstance(obj, dict):
        for k, v in obj.items():
            flatten(f"{prefix}.{k}" if prefix else k, v, out)
    elif isinstance(obj, list):
        s = ", ".join([str(x) for x in obj[:10]])
        if len(obj) > 10:
            s += " …"
        out[prefix] = s
    else:
        out[prefix] = obj

def build_rows_dynamic(all_events, tz_name="Asia/Seoul"):
    """
    동적 컬럼 테이블 변환:
    - timestamp(KST) 밀리초 표시
    - 자주 쓰는 필드 별칭 + 전체 평탄화 병합
    """
    rows = []
    for e in all_events:
        attrs = e.get("attributes", {}) or {}

        # 기본 필드
        row = {}
        row["timestamp(KST)"] = iso_to_kst_ms(attrs.get("timestamp"), tz_name)
        row["type"] = attrs.get("type")
        row["service"] = attrs.get("service")

        # 전체 평탄화
        flat = {}
        flatten("", attrs, flat)

        # 별칭(사람이 자주 보는 필드)
        aliases = {
            "application.id": flat.get("application.id"),
            "session.id": flat.get("session.id"),
            "session.type": flat.get("session.type"),
            "view.url": flat.get("view.url"),
            "view.referrer": flat.get("view.referrer"),
            "usr.id": flat.get("usr.id"),
            "usr.name": flat.get("usr.name"),
            "usr.email": flat.get("usr.email"),
            "action.type": flat.get("action.type"),
            "action.target.name": flat.get("action.target.name"),
            "resource.type": flat.get("resource.type"),
            "resource.url": flat.get("resource.url"),
            "error.message": flat.get("error.message"),
            "error.source": flat.get("error.source"),
            "error.stack": flat.get("error.stack"),
            "device.type": flat.get("device.type"),
            "os.name": flat.get("os.name"),
            "browser.name": flat.get("browser.name"),
            "attribute.os.build": flat.get("attribute.os.build"),
        }

        # row.update(aliases)
        row.update(flat)
        rows.append(row)
    return rows

def search_rum_events_usr_id(usr_id_value: str, minutes: int = 10, limit_per_page: int = 200, max_pages: int = 5, tz_name: str = "Asia/Seoul"):
    """
    Datadog RUM 이벤트 검색:
    - 기간: now-<minutes>m ~ now
    - 필터: @usr.id:"..." (비어있으면 전체)
    - 페이지네이션: 커서 따라가며 max_pages까지 수집
    """
    body = {
        "filter": {
            "from": f"now-{minutes}m",
            "to": "now",
            "query": build_usr_query(usr_id_value),
        },
        "page": {"limit": limit_per_page},
        "sort": "-timestamp",
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "DD-API-KEY": DD_API_KEY,
        "DD-APPLICATION-KEY": DD_APP_KEY,
    }

    all_events = []
    cursor = None

    for _ in range(max_pages):
        if cursor:
            body["page"]["cursor"] = cursor
        resp = requests.post(SEARCH_URL, headers=headers, data=json.dumps(body), timeout=30)
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

    # 간단 행(미사용이어도 반환 형식 유지)
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

def to_base_dataframe(raw, tz_name="Asia/Seoul"):
    """raw 이벤트 리스트 → 평탄화 DataFrame 생성 + timestamp 정렬"""
    dyn_rows = build_rows_dynamic(raw, tz_name=tz_name)
    df = pd.DataFrame(dyn_rows)

    if "timestamp(KST)" in df.columns:
        parsed_ts = pd.to_datetime(
            df["timestamp(KST)"].str.replace(" KST", "", regex=False),
            format="%Y-%m-%d %H:%M:%S.%f",
            errors="coerce"
        )
        df = df.assign(_ts=parsed_ts).sort_values("_ts", ascending=False).drop(columns=["_ts"])
    return df

# =======================
# UI (좌측: 조회 & 컬럼 제어, 우측: 결과)
# =======================
# --- 사이드바: 조회 폼 ---
with st.sidebar:
    st.markdown("### 검색 조건")
    with st.form(key="search_form", clear_on_submit=False):
        usr_id = st.text_input("usr.id", value="", placeholder="예: user_1234 (비우면 전체 *)")
        minutes = st.number_input("최근 분(min)", min_value=1, max_value=1440, value=10, step=1)
        limit_per_page = st.slider("페이지당 개수(limit)", min_value=50, max_value=1000, value=200, step=50)
        max_pages = st.slider("최대 페이지 수", min_value=1, max_value=20, value=5, step=1)
        run = st.form_submit_button("조회")   # ← 이 버튼 눌러야만 API 호출

    st.divider()

    # --- 보기 설정 폼 (재조회 없이 DF만 갱신) ---
    st.markdown("### 보기 설정")
    with st.form(key="view_form", clear_on_submit=False):
        auto_hide_sparse = st.checkbox("희소 컬럼 자동 숨김", value=True)
        sparse_threshold = st.slider("비어있지 않은 비율 기준(%)", min_value=0, max_value=50, value=5, step=1)

        # df_base가 있을 때만 컬럼 멀티셀렉트 표시
        if st.session_state.get("df_base") is not None:
            all_cols = [c for c in st.session_state.df_base.columns if c != "timestamp(KST)"]
            default_select = [c for c in st.session_state.hide_defaults if c in all_cols]
            hidden_cols = st.multiselect(
                "숨길 컬럼들",
                options=sorted(all_cols),
                default=default_select,
                help="선택 후 '보기 적용'을 눌러 반영"
            )
        else:
            hidden_cols = []
            st.info("먼저 '조회'를 실행하세요.")

        apply_view = st.form_submit_button("보기 적용")  # ← 이 버튼 눌러야만 표 갱신


st.write(f"**Site:** `{DD_SITE}` · **Endpoint:** `{SEARCH_URL}`")

# =======================
# 조회 실행
# =======================
# 조회 버튼 처리 (그대로)
if run:
    with st.spinner("검색 중..."):
        rows, raw = search_rum_events_usr_id(
            usr_id_value=usr_id,
            minutes=int(minutes),
            limit_per_page=int(limit_per_page),
            max_pages=int(max_pages),
            tz_name="Asia/Seoul",
        )
    if rows:
        st.session_state.df_base = to_base_dataframe(raw, tz_name="Asia/Seoul")
    else:
        st.session_state.df_base = None

# 보기 적용 버튼을 누를 때만 테이블 변환/렌더
if st.session_state.get("df_base") is not None and (run or apply_view):
    df_view = st.session_state.df_base.copy()

    # 희소 컬럼 자동 숨김
    if auto_hide_sparse and not df_view.empty:
        non_empty_ratio = (df_view.notna() & (df_view != "")).mean(numeric_only=False)
        keep_cols_sparse = [c for c in df_view.columns
                            if (non_empty_ratio.get(c, 0) * 100) >= sparse_threshold or c == "timestamp(KST)"]
        df_view = df_view[keep_cols_sparse]

    # 숨길 컬럼 적용
    drop_cols = [c for c in hidden_cols if c in df_view.columns and c != "timestamp(KST)"]
    if drop_cols:
        df_view = df_view.drop(columns=drop_cols, errors="ignore")

    st.dataframe(df_view, use_container_width=True)

else:
    st.caption("조회 실행 후 결과가 나타납니다.")
