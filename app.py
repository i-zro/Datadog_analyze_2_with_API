import streamlit as st
from datetime import datetime, timedelta
import pytz

# RUM modules
from rum.config import get_settings, get_default_hidden_columns
from rum.datadog_api import search_rum_events_usr_id
from rum.transform import build_rows_dynamic, to_base_dataframe, apply_view_filters, summarize_calls
from rum.ui import render_sidebar, render_main_view
from rum.helpers import effective_hidden, sanitize_pin_slots

# ─────────────────────────────────────────
# Constants & Settings
# ─────────────────────────────────────────
FIXED_PIN = "attributes.resource.url_path"  # 테이블에서 항상 고정할 열 이름
PIN_COUNT = 10  # 고정할 수 있는 최대 열 개수

# ─────────────────────────────────────────
# Session State & Data Processing
# ─────────────────────────────────────────
def initialize_session_state():
    """Streamlit의 세션 상태를 초기화합니다."""
    ss = st.session_state
    # 기본값 설정
    defaults = {
        "df_base": None,  # 원본 데이터프레임
        "df_view": None,  # 필터링 및 정렬된 뷰 데이터프레임
        "df_summary": None,  # 통화 요약 데이터프레임
        "hide_defaults": get_default_hidden_columns(),  # 기본적으로 숨길 열 목록
        "hidden_cols_user": [],  # 사용자가 선택한 숨길 열 목록
        "table_height": 900,  # 테이블 높이
        "pin_slots": [""] * PIN_COUNT,  # 고정 열 슬롯
    }
    for key, value in defaults.items():
        if key not in ss:
            ss[key] = value

    # UI 상태 초기화 (선택 대기 값)
    if "pending_hidden_cols_user" not in ss:
        ss.pending_hidden_cols_user = ss.hidden_cols_user.copy()
    if "pending_pin_slots" not in ss:
        ss.pending_pin_slots = ss.pin_slots.copy()

    # 시간 범위 초기화 (KST 기준)
    kst = pytz.timezone("Asia/Seoul")
    if "start_dt" not in ss:
        ss.start_dt = datetime.now(kst) - timedelta(minutes=10)
    if "end_dt" not in ss:
        ss.end_dt = datetime.now(kst)

def handle_search_and_process_data(settings, params):
    """API 검색을 실행하고 결과를 처리하여 세션 상태에 저장합니다."""
    ss = st.session_state
    with st.spinner("검색 중..."):
        # Datadog API를 통해 RUM 이벤트 검색
        raw_events = search_rum_events_usr_id(settings=settings, **params)
    st.success(f"가져온 이벤트: {len(raw_events)}건")

    if not raw_events:
        # 검색 결과가 없으면 세션 상태 초기화
        ss.df_base = ss.df_view = ss.df_summary = None
        st.info("검색 결과가 없습니다.")
        return

    # 이벤트 데이터를 데이터프레임으로 변환 및 가공
    with st.spinner("이벤트 데이터 변환 중..."):
        flat_rows = build_rows_dynamic(raw_events, tz_name="Asia/Seoul")
    
    with st.spinner("통화 정보 요약 중..."):
        ss.df_summary = summarize_calls(flat_rows)

    ss.df_base = to_base_dataframe(flat_rows)
    all_cols = [c for c in ss.df_base.columns if c != "timestamp(KST)"]
    
    # 새로운 데이터에 맞춰 표시 옵션 재설정
    ss.hidden_cols_user = [c for c in ss.hidden_cols_user if c in all_cols]
    ss.pending_hidden_cols_user = ss.hidden_cols_user.copy()
    
    visible_cols = [c for c in all_cols if c not in effective_hidden(all_cols, ss.pending_hidden_cols_user, ss.hide_defaults, FIXED_PIN)]
    ss.pin_slots = sanitize_pin_slots(ss.pin_slots, visible_cols, PIN_COUNT, FIXED_PIN)
    ss.pending_pin_slots = ss.pin_slots.copy()

    # 최종 뷰 생성
    eff_hidden_applied = effective_hidden(all_cols, ss.hidden_cols_user, ss.hide_defaults, FIXED_PIN)
    ss.df_view = apply_view_filters(ss.df_base.copy(), hidden_cols=eff_hidden_applied)

# ─────────────────────────────────────────
# Main App Logic
# ─────────────────────────────────────────
def main():
    """메인 애플리케이션 실행 함수"""
    st.set_page_config(page_title="Datadog RUM 분석기", layout="wide")
    st.title("📞 Datadog RUM 분석기")

    # 설정 로드
    try:
        settings = get_settings()
        st.write(f"**Site:** `{settings.site}`")
    except (KeyError, FileNotFoundError):
        st.error("설정 파일(.streamlit/secrets.toml)을 찾을 수 없거나 키가 누락되었습니다.")
        st.stop()

    # 세션 상태 초기화
    initialize_session_state()
    
    # 사이드바 렌더링 및 검색 파라미터 가져오기
    ss = st.session_state
    run_search, search_params = render_sidebar(ss, PIN_COUNT, FIXED_PIN)

    # "조회" 버튼 클릭 시 데이터 처리
    if run_search:
        handle_search_and_process_data(settings, search_params)
    
    # 메인 뷰 렌더링
    render_main_view(ss, PIN_COUNT, FIXED_PIN)

if __name__ == "__main__":
    main()
