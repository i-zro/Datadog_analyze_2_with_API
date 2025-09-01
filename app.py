import streamlit as st
import pandas as pd
from datetime import datetime, time, timedelta
import pytz

from rum.config import get_settings, get_search_url, get_default_hidden_columns
from rum.datadog_api import search_rum_events_usr_id
from rum.transform import build_rows_dynamic, to_base_dataframe, apply_view_filters, summarize_calls

st.set_page_config(page_title="Datadog RUM Search", layout="wide")
st.title("📞 Datadog RUM 분석기")
# ─────────────────────────────────────────
# 고정 핀(맨 왼쪽 두 칸: timestamp(KST) 다음)
# ─────────────────────────────────────────
FIXED_PIN = "attributes.resource.url_path"  # 존재하면 timestamp(KST) 다음에 항상 고정
PIN_COUNT = 10  # ✅ 핀 슬롯 수
# ─────────────────────────────────────────
# 설정/키 로드
# ─────────────────────────────────────────
settings = get_settings()
if not (settings.api_key and settings.app_key):
    st.error("DD_API_KEY / DD_APP_KEY가 설정되어 있지 않습니다. .streamlit/secrets.toml 또는 환경변수를 확인하세요.")
    st.stop()
SEARCH_URL = get_search_url(settings.site)

# ─────────────────────────────────────────
# 세션 상태 초기화
# ─────────────────────────────────────────
ss = st.session_state
if "df_base" not in ss:
    ss.df_base = None
if "df_view" not in ss:
    ss.df_view = None  # 렌더용 캐시(적용된 결과)
if "df_summary" not in ss:
    ss.df_summary = None # 통화 요약 데이터
if "hide_defaults" not in ss:
    ss.hide_defaults = get_default_hidden_columns()   # 기본 숨김(사이드바에 표시 X)
if "hidden_cols_user" not in ss:
    ss.hidden_cols_user = []                          # 적용된 사용자 숨김
if "table_height" not in ss:
    ss.table_height = 900
if "pin_slots" not in ss:
    ss.pin_slots = [""] * PIN_COUNT   # ✅ 10칸

# 선택(미적용, 대기값) 상태
if "pending_hidden_cols_user" not in ss:
    ss.pending_hidden_cols_user = ss.hidden_cols_user.copy()
if "pending_pin_slots" not in ss:
    ss.pending_pin_slots = ss.pin_slots.copy()  # ✅ 길이 10 유지

def effective_hidden(all_cols: list[str], user_hidden: list[str]) -> list[str]:
    """
    기본 숨김 + 사용자 숨김 -> 실제 존재하는 컬럼만.
    단, FIXED_PIN은 항상 표시되도록 최종 숨김에서 제외.
    """
    hidden = (set(ss.hide_defaults) | set(user_hidden)) & set(all_cols)
    if FIXED_PIN in hidden:
        hidden.remove(FIXED_PIN)
    return sorted(hidden)

def sanitize_pin_slots(slot_values: list[str], visible_candidates: list[str], count: int = PIN_COUNT) -> list[str]:
    """핀 슬롯값을 보이는 후보에 맞춰 정리(중복 제거, 순서 유지, 최대 count개)"""
    allow = set(visible_candidates)
    seen, out = set(), []
    for v in slot_values:
        c = (v or "").strip()
        if c and c in allow and c not in seen and c != FIXED_PIN:
            out.append(c); seen.add(c)
    out += [""] * (count - len(out))
    return out[:count]


# ─────────────────────────────────────────
# 사이드바 1: 검색 폼
# ─────────────────────────────────────────
with st.sidebar:
    st.markdown("### 검색 조건")
    usr_id = st.text_input("usr.id", value="", placeholder="예: user_1234 (비우면 전체 *)")

    # --- 시간 입력 --- #
    kst = pytz.timezone("Asia/Seoul")

    # 세션 상태에 시간 값 초기화
    if "start_dt" not in ss:
        ss.start_dt = datetime.now(kst) - timedelta(minutes=10)
    if "end_dt" not in ss:
        ss.end_dt = datetime.now(kst)

    st.markdown("##### 검색 기간 (KST)")
    col1, col2 = st.columns(2)
    with col1:
        start_date_val = st.date_input("시작 날짜", value=ss.start_dt.date())
        start_time_val = st.time_input("시작 시간", value=ss.start_dt.time())
    with col2:
        end_date_val = st.date_input("종료 날짜", value=ss.end_dt.date())
        end_time_val = st.time_input("종료 시간", value=ss.end_dt.time())

    # 입력 위젯의 현재 값으로 세션 상태 업데이트
    ss.start_dt = kst.localize(datetime.combine(start_date_val, start_time_val))
    ss.end_dt = kst.localize(datetime.combine(end_date_val, end_time_val))
    # --- 시간 입력 끝 --- #

    limit_per_page = st.slider("페이지당 개수(limit)", min_value=50, max_value=1000, value=200, step=50)
    max_pages = st.slider("최대 페이지 수", min_value=1, max_value=20, value=5, step=1)

    is_valid_time_range = ss.start_dt < ss.end_dt
    if not is_valid_time_range:
        st.error("시작 시간은 종료 시간보다 빨라야 합니다.")

    run = st.button("조회", disabled=not is_valid_time_range)

st.write(f"**Site:** `{settings.site}` · **Endpoint:** `{SEARCH_URL}`")

# ─────────────────────────────────────────
# 조회 실행
# ─────────────────────────────────────────
if run:
    from_ts_utc = ss.start_dt.astimezone(pytz.utc).isoformat()
    to_ts_utc = ss.end_dt.astimezone(pytz.utc).isoformat()

    with st.spinner("검색 중..."):
        raw_events = search_rum_events_usr_id(
            settings=settings,
            usr_id_value=usr_id,
            from_ts=from_ts_utc,
            to_ts=to_ts_utc,
            limit_per_page=int(limit_per_page),
            max_pages=int(max_pages),
        )
    st.success(f"가져온 이벤트: {len(raw_events)}건")

    if not raw_events:
        ss.df_base = None
        ss.df_view = None
        ss.df_summary = None
        st.info("검색 결과가 없습니다. usr.id 값과 지정된 시간 내 데이터 존재 여부를 확인하세요.")
    else:
        # 1. 모든 이벤트를 한 번만 평탄화하여 처리 효율을 높입니다.
        with st.spinner("이벤트 데이터 변환 중..."):
            flat_rows = build_rows_dynamic(raw_events, tz_name="Asia/Seoul")

        # 2. 통화 요약 정보 생성
        with st.spinner("통화 정보 요약 중..."):
            ss.df_summary = summarize_calls(flat_rows)

        # 3. 이벤트 로그 DataFrame 생성
        ss.df_base = to_base_dataframe(flat_rows)

        # 4. 이벤트 로그 뷰(표시 옵션 적용) 설정
        cols_now = [c for c in ss.df_base.columns if c != "timestamp(KST)"]
        ss.hidden_cols_user = [c for c in ss.hidden_cols_user if c in cols_now]
        ss.pending_hidden_cols_user = ss.hidden_cols_user.copy()
        eff_hidden_proposed = effective_hidden(cols_now, ss.pending_hidden_cols_user)
        visible_candidates_after = [c for c in cols_now if c not in eff_hidden_proposed and c != FIXED_PIN]
        ss.pin_slots = sanitize_pin_slots(ss.pin_slots, visible_candidates_after)
        ss.pending_pin_slots = ss.pin_slots.copy()
        eff_hidden_applied = effective_hidden(cols_now, ss.hidden_cols_user)
        ss.df_view = apply_view_filters(
            ss.df_base.copy(), auto_hide_sparse=False, sparse_threshold=0, hidden_cols=eff_hidden_applied
        )

# ─────────────────────────────────────────
# 사이드바 2: 숨김/핀 선택 + 보기 새로고침 (검색 후에만 렌더)
# ─────────────────────────────────────────
with st.sidebar:
    st.divider()
    st.markdown("### 표시 옵션")
    if ss.df_base is not None:
        ss.table_height = st.slider("표 높이(px)", min_value=400, max_value=2000, value=ss.table_height, step=50)

        # 현재 컬럼 목록
        all_cols = [c for c in ss.df_base.columns if c != "timestamp(KST)"]
        defaults_set = set(ss.hide_defaults)

        # ───── 1) 숨길 컬럼(선택) ─────
        st.markdown("### 숨길 컬럼(선택)")
        # 옵션: 기본 숨김 + FIXED_PIN 제외
        options_for_hide = sorted([c for c in all_cols if c not in defaults_set and c != FIXED_PIN])
        ss.pending_hidden_cols_user = st.multiselect(
            "숨길 컬럼(선택 후 '보기 새로고침'으로 적용)",
            options=options_for_hide,
            default=ss.pending_hidden_cols_user,
            help=f"기본 숨김 컬럼과 '{FIXED_PIN}'은 여기 표시되지 않습니다."
        )

        # ───── 2) 핀(왼쪽 고정) 순서(핀 #1 ~ #10) ─────
        st.markdown("### 핀(왼쪽 고정) 순서")
        st.caption(f"맨 왼쪽은 'timestamp(KST)' → 그 다음은 '{FIXED_PIN}'(존재 시) → 아래 슬롯 순서대로 고정")

        # 대기 숨김 기준으로 보이는 후보 계산
        eff_hidden_proposed = effective_hidden(all_cols, ss.pending_hidden_cols_user)
        visible_candidates_after = [c for c in all_cols if c not in eff_hidden_proposed and c != FIXED_PIN]

        # 슬롯값 보정(보이는 후보에 맞춰)
        ss.pending_pin_slots = sanitize_pin_slots(ss.pending_pin_slots, visible_candidates_after)

        slot_options = [""] + visible_candidates_after  # "" = 비움
        labels = [f"핀 #{i}" for i in range(1, PIN_COUNT + 1)]

        # ✅ 세로 한 줄로 10개 나열
        for i, lab in enumerate(labels):
            ss.pending_pin_slots[i] = st.selectbox(
                lab,
                options=slot_options,
                index=slot_options.index(ss.pending_pin_slots[i]) if ss.pending_pin_slots[i] in slot_options else 0,
                key=f"pending_pin_slot_{i}",
                help="왼쪽부터 고정할 순서를 지정합니다. 비워두면 건너뜁니다."
            )

        # ───── 3) 보기 새로고침 (적용 버튼) ─────
        apply_view = st.button("보기 새로고침")
        if apply_view:
            ss.hidden_cols_user = ss.pending_hidden_cols_user.copy()
            ss.pin_slots = sanitize_pin_slots(ss.pending_pin_slots, visible_candidates_after)
            eff_hidden_applied = effective_hidden(all_cols, ss.hidden_cols_user)
            ss.df_view = apply_view_filters(
                ss.df_base.copy(), auto_hide_sparse=False, sparse_threshold=0, hidden_cols=eff_hidden_applied
            )
            st.rerun()

        reset_user_hide = st.button("모두 표시(사용자 숨김 초기화)")
        if reset_user_hide:
            ss.pending_hidden_cols_user = []
            ss.pending_pin_slots = ss.pin_slots.copy()
            ss.hidden_cols_user = []
            eff_hidden_applied = effective_hidden(all_cols, ss.hidden_cols_user)
            ss.df_view = apply_view_filters(
                ss.df_base.copy(), auto_hide_sparse=False, sparse_threshold=0, hidden_cols=eff_hidden_applied
            )
            st.rerun()
    else:
        st.info("먼저 좌측에서 '조회'를 실행하세요.")

# ─────────────────────────────────────────
# 렌더: 고정 핀 적용 + 표
# ─────────────────────────────────────────
def reorder_for_pinned(df: pd.DataFrame, fixed_second: str, pin_slots: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    pins, seen = [], set()
    # 1) timestamp(KST)
    if "timestamp(KST)" in df.columns:
        pins.append("timestamp(KST)"); seen.add("timestamp(KST)")
    # 2) 고정 칸(FIXED_PIN) - 존재하면 항상 두 번째
    if fixed_second in df.columns and fixed_second not in seen:
        pins.append(fixed_second); seen.add(fixed_second)
    # 3) 슬롯 핀(1~4)
    for c in pin_slots:
        if c and c in df.columns and c not in seen:
            pins.append(c); seen.add(c)
    # 4) 나머지
    rest = [c for c in df.columns if c not in seen]
    return df[pins + rest]

if ss.df_view is not None:
    # 1. 통화 분석 섹션 렌더링
    if ss.df_summary is not None and not ss.df_summary.empty:
        st.markdown("## 📞 통화 분석")
        st.dataframe(ss.df_summary, use_container_width=True)
        st.divider()

    # 2. 이벤트 로그 섹션 렌더링
    st.markdown("## 📄 이벤트 로그")
    df_render = reorder_for_pinned(ss.df_view, FIXED_PIN, ss.pin_slots)
    st.dataframe(df_render, use_container_width=True, height=ss.table_height)

    with st.expander("원본 이벤트(JSON) 보기"):
        show_json = st.checkbox("JSON 변환/표시", value=False)
        if show_json:
            st.json(df_render.head(50).to_dict(orient="records"))
else:
    st.caption("조회 실행 후 결과가 나타납니다.")
