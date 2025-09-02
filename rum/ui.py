import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pytz

from .helpers import effective_hidden, sanitize_pin_slots, reorder_for_pinned, filter_dataframe, apply_row_highlighting
from .transform import apply_view_filters

def render_sidebar(ss, pin_count, fixed_pin):
    """
    사이드바 UI를 렌더링하고 검색 파라미터를 반환합니다.
    """
    with st.sidebar:
        st.markdown("### 검색 조건")
        usr_id = st.text_input("usr.id", value="", placeholder="예: user_1234 (비우면 전체 *)")

        kst = pytz.timezone("Asia/Seoul")
        st.markdown("##### 검색 기간 (KST)")
        col1, col2 = st.columns(2)
        start_date = col1.date_input("시작 날짜", value=ss.start_dt.date())
        start_time = col1.time_input("시작 시간", value=ss.start_dt.time())
        end_date = col2.date_input("종료 날짜", value=ss.end_dt.date())
        end_time = col2.time_input("종료 시간", value=ss.end_dt.time())

        ss.start_dt = kst.localize(datetime.combine(start_date, start_time))
        ss.end_dt = kst.localize(datetime.combine(end_date, end_time))

        limit_per_page = st.slider("페이지당 개수(limit)", 50, 1000, 200, 50)
        max_pages = st.slider("최대 페이지 수", 1, 20, 5, 1)

        is_valid_time = ss.start_dt < ss.end_dt
        if not is_valid_time:
            st.error("시작 시간은 종료 시간보다 빨라야 합니다.")
        
        run_search = st.button("조회", disabled=not is_valid_time)

        st.divider()
        st.markdown("### 표시 옵션")
        if ss.df_base is not None:
            render_options_sidebar(ss, pin_count, fixed_pin)
        else:
            st.info("먼저 '조회'를 실행하세요.")

    search_params = {
        "usr_id_value": usr_id,
        "from_ts": ss.start_dt.astimezone(pytz.utc).isoformat(),
        "to_ts": ss.end_dt.astimezone(pytz.utc).isoformat(),
        "limit_per_page": limit_per_page,
        "max_pages": max_pages,
    }
    return run_search, search_params

def render_options_sidebar(ss, pin_count, fixed_pin):
    """사이드바에 표시 옵션(열 숨김, 핀 설정 등)을 렌더링합니다."""
    ss.table_height = st.slider("표 높이(px)", 400, 2000, ss.table_height, 50)

    all_cols = [c for c in ss.df_base.columns if c != "timestamp(KST)"]
    defaults_set = set(ss.hide_defaults)
    options_for_hide = sorted([c for c in all_cols if c not in defaults_set and c != fixed_pin])

    st.markdown("### 숨길 컬럼(선택)")
    ss.pending_hidden_cols_user = st.multiselect(
        "숨길 컬럼", options_for_hide, default=ss.pending_hidden_cols_user, label_visibility="collapsed"
    )

    st.markdown("### 핀(왼쪽 고정) 순서")
    eff_hidden_proposed = effective_hidden(all_cols, ss.pending_hidden_cols_user, ss.hide_defaults, fixed_pin)
    visible_candidates = [c for c in all_cols if c not in eff_hidden_proposed and c != fixed_pin]
    ss.pending_pin_slots = sanitize_pin_slots(ss.pending_pin_slots, visible_candidates, pin_count, fixed_pin)
    slot_options = [""] + visible_candidates

    for i in range(pin_count):
        ss.pending_pin_slots[i] = st.selectbox(
            f"핀 #{i+1}", options=slot_options, 
            index=slot_options.index(ss.pending_pin_slots[i]) if ss.pending_pin_slots[i] in slot_options else 0,
            key=f"pin_{i}"
        )

    if st.button("보기 새로고침"):
        ss.hidden_cols_user = ss.pending_hidden_cols_user.copy()
        ss.pin_slots = sanitize_pin_slots(ss.pending_pin_slots, visible_candidates, pin_count, fixed_pin)
        eff_hidden_applied = effective_hidden(all_cols, ss.hidden_cols_user, ss.hide_defaults, fixed_pin)
        ss.df_view = apply_view_filters(ss.df_base.copy(), hidden_cols=eff_hidden_applied)
        st.rerun()

    if st.button("모두 표시(사용자 숨김 초기화)"):
        ss.pending_hidden_cols_user = []
        ss.hidden_cols_user = []
        eff_hidden_applied = effective_hidden(all_cols, [], ss.hide_defaults, fixed_pin)
        ss.df_view = apply_view_filters(ss.df_base.copy(), hidden_cols=eff_hidden_applied)
        st.rerun()

def render_main_view(ss, pin_count, fixed_pin):
    """메인 화면(통화 분석, 이벤트 로그)을 렌더링합니다."""
    if ss.df_view is not None:
        if ss.df_summary is not None and not ss.df_summary.empty:
            st.markdown("## 📞 통화 분석")
            st.dataframe(ss.df_summary, use_container_width=True)
            st.divider()

        st.markdown("## 📄 이벤트 로그")
        
        # --- 필터 및 하이라이트 UI ---
        col1, col2 = st.columns([4, 1])
        filter_text = col1.text_input("URL Path 필터", placeholder="쉼표(,)로 구분하여 여러 개 입력")
        is_and = col2.checkbox("AND 조건", help="모든 키워드를 포함하는 로그만 필터링")
        
        st.markdown("##### 행 하이라이트 (쉼표로 구분, OR 조건)")
        h_col1, h_col2, h_col3 = st.columns(3)
        red_kws = h_col1.text_input("🔴 빨강", placeholder="빨강으로 강조할 키워드")
        blue_kws = h_col2.text_input("🔵 파랑", placeholder="파랑으로 강조할 키워드")
        yellow_kws = h_col3.text_input("🟡 노랑", placeholder="노랑으로 강조할 키워드")
        # --- 필터 및 하이라이트 UI 끝 ---

        df_render = reorder_for_pinned(ss.df_view, fixed_pin, ss.pin_slots)
        
        if filter_text:
            df_render = filter_dataframe(df_render, fixed_pin, filter_text, is_and)

        # 하이라이트 적용
        styler = apply_row_highlighting(df_render, red_kws, blue_kws, yellow_kws)

        st.dataframe(styler, use_container_width=True, height=ss.table_height)
        
        with st.expander("원본 이벤트(JSON) 보기"):
            if st.checkbox("JSON 변환/표시"):
                st.json(df_render.head(50).to_dict(orient="records"))
    else:
        st.caption("조회 실행 후 결과가 나타납니다.")
