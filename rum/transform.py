from datetime import datetime, timedelta
from dateutil import tz
from typing import Dict, Any, List
import pandas as pd
from collections import defaultdict

# ─────────────────────────────────────────
# 데이터 변환 함수
# ─────────────────────────────────────────

def iso_to_kst_ms(iso_str: str, tz_name: str = "Asia/Seoul") -> str:
    """ISO 8601 형식의 시간 문자열을 KST 시간(ms 단위 포함)으로 변환합니다."""
    if not iso_str:
        return ""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    kst = tz.gettz(tz_name)
    k = dt.astimezone(kst)
    return k.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(k.strftime('%f'))//1000:03d} KST"

def flatten(prefix: str, obj: Any, out: Dict[str, Any]) -> None:
    """중첩된 딕셔너리나 리스트를 평탄화하여 단일 딕셔너리로 만듭니다."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            flatten(f"{prefix}.{k}" if prefix else k, v, out)
    elif isinstance(obj, list):
        out[prefix] = ", ".join(map(str, obj))
    else:
        out[prefix] = obj

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
        flat_row: Dict[str, Any] = {}
        flatten("", attrs, flat_row)
        flat_row["timestamp(KST)"] = iso_to_kst_ms(attrs.get("timestamp"), tz_name)

        call_id_val = (
            flat_row.get("attributes.context.callID")
            or flat_row.get("attributes.context.callId")
            or flat_row.get("attributes.context.CallIDs")
        )

        if call_id_val is not None:
            flat_row["Call ID"] = call_id_val
            flat_row.pop("attributes.context.callID", None)
            flat_row.pop("attributes.context.callId", None)
            flat_row.pop("attributes.context.CallIDs", None)

        processed_rows.append(flat_row)
    return processed_rows

def summarize_calls(flat_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    RUM 이벤트를 Call ID별로 그룹화하고 통화 정보를 요약합니다.
    - 종료 사유, 패킷 정보, 주요 이벤트 상태 코드를 추출하여 요약 테이블을 생성합니다.
    """
    calls = defaultdict(list)
    for row in flat_rows:
        call_id = row.get("Call ID")
        if call_id:
            calls[call_id].append(row)

    if not calls:
        return pd.DataFrame()

    summaries = []
    for call_id, events in calls.items():
        termination_reason = None
        send_packets = []
        receive_packets = []
        request_status, accept_status, reject_status, end_status = None, None, None, None
        active_ts, stopping_ts = None, None

        overall_end_time_str = events[0].get("timestamp(KST)")
        overall_start_time_str = events[-1].get("timestamp(KST)")

        for event in events:
            path = event.get("attributes.resource.url_path")
            status_code = event.get("attributes.resource.status_code")
            timestamp_str = event.get("timestamp(KST)")

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
            elif path == "/res/ENGINE_ReceivePackets" and len(receive_packets) < 3:
                count = event.get("attributes.context.totalCount")
                if count is not None:
                    receive_packets.append(count)
        
        duration_str = ""
        if stopping_ts is None:
            duration_str = "STOPPING 없음"
        elif active_ts is None:
            duration_str = "ACTIVE 없음"
        else:
            try:
                stop_dt = datetime.strptime(stopping_ts.replace(" KST", ""), "%Y-%m-%d %H:%M:%S.%f")
                active_dt = datetime.strptime(active_ts.replace(" KST", ""), "%Y-%m-%d %H:%M:%S.%f")
                duration_seconds = (stop_dt - active_dt).total_seconds()
                duration_str = f"{duration_seconds:.1f} 초"
            except (ValueError, TypeError):
                duration_str = "시간 포맷 오류"

        summaries.append({
            "Call ID": call_id,
            "Start Time (KST)": overall_start_time_str,
            "End Time (KST)": overall_end_time_str,
            "Duration": duration_str,
            "종료 사유": termination_reason,
            "requestVoiceCall_status_code": request_status,
            "acceptCall_status_code": accept_status,
            "rejectCall_status_code": reject_status,
            "endCall_status_code": end_status,
            "SendPackets 수": send_packets,
            "ReceivePackets 수": receive_packets,
        })

    summary_df = pd.DataFrame(summaries)
    if not summary_df.empty and "Start Time (KST)" in summary_df.columns:
        summary_df = summary_df.sort_values("Start Time (KST)", ascending=False).reset_index(drop=True)

    return summary_df

def to_base_dataframe(flat_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """평탄화된 행 목록으로부터 DataFrame을 생성하고, 데이터 타입을 변환한 후 시간순으로 정렬합니다."""
    df = pd.DataFrame(flat_rows)

    for col in ["attributes.resource.status_code", "attributes.resource.duration"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    if "timestamp(KST)" in df.columns:
        parsed_ts = pd.to_datetime(
            df["timestamp(KST)"].str.replace(" KST", "", regex=False),
            format="%Y-%m-%d %H:%M:%S.%f",
            errors="coerce"
        )
        df = df.assign(_ts=parsed_ts).sort_values("_ts", ascending=False).drop(columns=["_ts"])
    return df

def apply_view_filters(
    df_view: pd.DataFrame,
    auto_hide_sparse: bool = False,
    sparse_threshold: int = 5,
    hidden_cols: List[str] = None,
) -> pd.DataFrame:
    """
    데이터프레임에 뷰 관련 필터(희소 열 숨김, 사용자 지정 숨김)를 적용합니다.
    - auto_hide_sparse: True일 경우, 값이 거의 없는 열(희소 열)을 자동으로 숨깁니다. 기본값은 False입니다.
    """
    hidden_cols = hidden_cols or []
    if auto_hide_sparse and sparse_threshold > 0 and not df_view.empty:
        non_empty_ratio = (df_view.notna() & (df_view != "")).mean(numeric_only=False)
        keep_cols_sparse = [
            c for c in df_view.columns
            if (non_empty_ratio.get(c, 0) * 100) >= sparse_threshold or c == "timestamp(KST)"
        ]
        df_view = df_view[keep_cols_sparse]

    drops = [c for c in hidden_cols if c in df_view.columns and c != "timestamp(KST)"]
    if drops:
        df_view = df_view.drop(columns=drops, errors="ignore")

    return df_view
