from datetime import datetime, timedelta
from dateutil import tz
from typing import Dict, Any, List
import pandas as pd
from collections import defaultdict

# ----- 시간 변환 -----
def iso_to_kst_ms(iso_str: str, tz_name: str = "Asia/Seoul") -> str:
    if not iso_str:
        return ""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    kst = tz.gettz(tz_name)
    k = dt.astimezone(kst)
    return k.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(k.strftime('%f'))//1000:03d} KST"

# ----- 평탄화 -----
def flatten(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
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

# ----- 행 생성 -----
def build_rows_dynamic(all_events: List[Dict[str, Any]], tz_name="Asia/Seoul") -> List[Dict[str, Any]]:
    """
    RUM 이벤트 목록을 평탄화된 행(딕셔너리)의 목록으로 변환합니다.
    - 중첩된 속성을 'a.b.c' 형태로 평탄화합니다.
    - 타임스탬프를 KST로 변환합니다.
    - 여러 형태의 Call ID를 단일 필드로 통합합니다.
    """
    processed_rows: List[Dict[str, Any]] = []
    for event in all_events:
        attrs = event.get("attributes", {}) or {}

        # 1. 모든 속성을 평탄화합니다.
        flat_row: Dict[str, Any] = {}
        flatten("", attrs, flat_row)

        # 2. 타임스탬프를 KST로 변환하여 'timestamp(KST)' 키에 저장합니다.
        flat_row["timestamp(KST)"] = iso_to_kst_ms(attrs.get("timestamp"), tz_name)

        # 3. Call ID를 통합합니다.
        # Datadog RUM 이벤트 구조상 custom attribute는 `attributes.attributes` 내부에 위치하므로,
        # 평탄화된 키는 'attributes.' 접두사를 갖게 됩니다.
        call_id_val = (
            flat_row.get("attributes.context.callID")
            or flat_row.get("attributes.context.callId")
        )

        if call_id_val is not None:
            flat_row["Call ID"] = call_id_val
            # 기존 키를 제거하여 중복을 방지합니다.
            flat_row.pop("attributes.context.callID", None)
            flat_row.pop("attributes.context.callId", None)

        processed_rows.append(flat_row)
    return processed_rows

# ----- 통화 요약 -----
def summarize_calls(flat_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    RUM 이벤트를 Call ID별로 그룹화하고 통화 정보를 요약합니다.

    - 종료 사유: SDK_CALL_STATUS_STOPPING 이벤트에서 추출
    - Send/Receive Packets: ENGINE_SendPackets/ReceivePackets 이벤트에서 최근 3개의 totalCount 추출
    """
    # 1. 먼저 모든 이벤트를 평탄화된 행으로 변환합니다.
    # 2. 'Call ID'를 기준으로 이벤트를 그룹화합니다.
    calls = defaultdict(list)
    for row in flat_rows:
        call_id = row.get("Call ID")
        if call_id:
            calls[call_id].append(row)

    if not calls:
        return pd.DataFrame()

    # 3. 각 통화 그룹을 처리하여 요약 정보를 생성합니다.
    summaries = []
    for call_id, events in calls.items():
        # events 리스트는 이미 최신순으로 정렬되어 있습니다.
        termination_reason = None
        send_packets = []
        receive_packets = []

        # 가장 최근 이벤트에서 공통 정보(예: usr.id)를 가져옵니다.
        first_event = events[0]
        usr_id = first_event.get("usr.id")

        # 통화 시작 및 종료 시간을 계산합니다.
        # 리스트가 최신순이므로, 0번 인덱스가 종료, 마지막 인덱스가 시작입니다.
        end_time_str = events[0].get("timestamp(KST)")
        start_time_str = events[-1].get("timestamp(KST)")

        duration_str = "N/A"
        try:
            # " KST"를 제거하고 datetime 객체로 파싱합니다. 포맷: 2024-01-01 12:34:56.789
            end_dt = datetime.strptime(end_time_str.replace(" KST", ""), "%Y-%m-%d %H:%M:%S.%f")
            start_dt = datetime.strptime(start_time_str.replace(" KST", ""), "%Y-%m-%d %H:%M:%S.%f")
            duration = end_dt - start_dt
            # 초 단위까지만 깔끔하게 표시합니다.
            duration_str = str(duration - timedelta(microseconds=duration.microseconds))
        except (ValueError, TypeError, AttributeError):
            pass  # 파싱 실패 시 "N/A" 유지

        for event in events:
            path = event.get("attributes.resource.url_path")

            if path == "/res/SDK_CALL_STATUS_STOPPING" and termination_reason is None:
                termination_reason = event.get("attributes.context.eventType")

            if path == "/res/ENGINE_SendPackets" and len(send_packets) < 3:
                count = event.get("attributes.context.totalCount")
                if count is not None:
                    send_packets.append(count)

            # 참고: 요청에 개수 제한이 명시되지 않았으나, 일관성을 위해 최근 3개로 제한합니다.
            if path == "/res/ENGINE_ReceivePackets" and len(receive_packets) < 3:
                count = event.get("attributes.context.totalCount")
                if count is not None:
                    receive_packets.append(count)

        summaries.append({
            "Call ID": call_id,
            "User ID": usr_id,
            "Start Time (KST)": start_time_str,
            "End Time (KST)": end_time_str,
            "Duration": duration_str,
            "Termination Reason": termination_reason,
            "SendPackets Counts (last 3)": send_packets,
            "ReceivePackets Counts (last 3)": receive_packets,
        })

    summary_df = pd.DataFrame(summaries)
    if not summary_df.empty and "Start Time (KST)" in summary_df.columns:
        summary_df = summary_df.sort_values("Start Time (KST)", ascending=False).reset_index(drop=True)

    return summary_df

# ----- DataFrame 생성/정렬 -----
def to_base_dataframe(flat_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """평탄화된 행 목록으로부터 DataFrame을 생성하고 시간순으로 정렬합니다."""
    df = pd.DataFrame(flat_rows)

    if "timestamp(KST)" in df.columns:
        parsed_ts = pd.to_datetime(
            df["timestamp(KST)"].str.replace(" KST", "", regex=False),
            format="%Y-%m-%d %H:%M:%S.%f",
            errors="coerce"
        )
        df = df.assign(_ts=parsed_ts).sort_values("_ts", ascending=False).drop(columns=["_ts"])
    return df

# ----- 뷰 필터 적용 (희소 컬럼 + 숨김 컬럼) -----
def apply_view_filters(
    df_view: pd.DataFrame,
    auto_hide_sparse: bool = True,
    sparse_threshold: int = 5,   # percent
    hidden_cols: List[str] = None,
) -> pd.DataFrame:
    hidden_cols = hidden_cols or []

    # ▼ 희소 컬럼 자동 숨김: 비활성 또는 기준 0%면 스킵
    if auto_hide_sparse and sparse_threshold > 0 and not df_view.empty:
        non_empty_ratio = (df_view.notna() & (df_view != "")).mean(numeric_only=False)
        keep_cols_sparse = [
            c for c in df_view.columns
            if (non_empty_ratio.get(c, 0) * 100) >= sparse_threshold or c == "timestamp(KST)"
        ]
        df_view = df_view[keep_cols_sparse]

    # 멀티셀렉트로 숨기기로 한 컬럼 제거 (timestamp는 항상 유지)
    drops = [c for c in hidden_cols if c in df_view.columns and c != "timestamp(KST)"]
    if drops:
        df_view = df_view.drop(columns=drops, errors="ignore")

    return df_view
