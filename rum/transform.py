from datetime import datetime
from dateutil import tz
from typing import Dict, Any, List
import pandas as pd

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
    rows: List[Dict[str, Any]] = []
    for e in all_events:
        attrs = e.get("attributes", {}) or {}

        row: Dict[str, Any] = {}
        row["timestamp(KST)"] = iso_to_kst_ms(attrs.get("timestamp"), tz_name)
        row["type"] = attrs.get("type")
        row["service"] = attrs.get("service")

        flat: Dict[str, Any] = {}
        flatten("", attrs, flat)

        # [수정] callID 관련 필드를 `context.call_id`로 통합합니다.
        # Datadog RUM 이벤트 구조상 custom attribute는 `attributes.attributes` 내부에 위치하므로,
        # 평탄화된 키는 'attributes.' 접두사를 갖게 됩니다.
        call_id_val = (
            flat.get("attributes.context.callID")
            or flat.get("attributes.context.callId")
            or flat.get("attributes.context.CallIDs")
        )

        if call_id_val:
            # 'context.call_id' 라는 일관된 이름의 새 키를 생성합니다.
            flat["Call ID"] = call_id_val
            # 데이터 중복과 혼동을 막기 위해 사용된 모든 기존 키를 제거합니다.
            for key in [
                "attributes.context.callID",
                "attributes.context.callId",
                "attributes.context.CallIDs",
            ]:
                flat.pop(key, None)

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

# ----- DataFrame 생성/정렬 -----
def to_base_dataframe(raw: List[Dict[str, Any]], tz_name="Asia/Seoul") -> pd.DataFrame:
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
