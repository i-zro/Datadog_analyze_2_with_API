import streamlit as st
import pandas as pd
import re

def effective_hidden(all_cols: list[str], user_hidden: list[str], hide_defaults: list[str], fixed_pin: str) -> list[str]:
    """
    실제로 숨겨야 할 열 목록을 계산합니다.
    기본 숨김 목록과 사용자가 선택한 숨김 목록을 합치고, 고정 열은 제외합니다.
    """
    hidden = (set(hide_defaults) | set(user_hidden)) & set(all_cols)
    if fixed_pin in hidden:
        hidden.remove(fixed_pin)
    return sorted(list(hidden))

def sanitize_pin_slots(slot_values: list[str], visible_candidates: list[str], count: int, fixed_pin: str) -> list[str]:
    """
    핀 슬롯 값을 정리하고 유효한 값만 남깁니다.
    중복을 제거하고, 보이는 열 목록에 있는 값만 유지하며, 최대 개수를 맞춥니다.
    """
    allow = set(visible_candidates)
    seen, out = set(), []
    for v in slot_values:
        c = (v or "").strip()
        if c and c in allow and c not in seen and c != fixed_pin:
            out.append(c)
            seen.add(c)
    out += [""] * (count - len(out))
    return out[:count]

def reorder_for_pinned(df: pd.DataFrame, fixed_second: str, pin_slots: list[str]) -> pd.DataFrame:
    """
    고정 핀 설정에 따라 데이터프레임의 열 순서를 재정렬합니다.
    """
    if df is None or df.empty:
        return df
    
    pins, seen = [], set()
    if "timestamp(KST)" in df.columns:
        pins.append("timestamp(KST)")
        seen.add("timestamp(KST)")
    if fixed_second in df.columns and fixed_second not in seen:
        pins.append(fixed_second)
        seen.add(fixed_second)
    for c in pin_slots:
        if c and c in df.columns and c not in seen:
            pins.append(c)
            seen.add(c)
    rest = [c for c in df.columns if c not in seen]
    return df[pins + rest]

def filter_dataframe(df: pd.DataFrame, column: str, filter_text: str, is_and: bool) -> pd.DataFrame:
    """
    주어진 조건에 따라 데이터프레임을 필터링합니다.
    """
    if column not in df.columns:
        st.warning(f"'{column}' 컬럼이 없어 필터링할 수 없습니다.")
        return df

    keywords = [kw.strip() for kw in filter_text.split(',') if kw.strip()]
    if not keywords:
        return df

    series = df[column].fillna('')
    
    if is_and:
        condition = pd.Series(True, index=df.index)
        for kw in keywords:
            condition &= series.str.contains(re.escape(kw), case=False, regex=True)
    else:
        regex_pattern = '|'.join(re.escape(kw) for kw in keywords)
        condition = series.str.contains(regex_pattern, case=False, regex=True)
    
    return df[condition]

def apply_row_highlighting(df: pd.DataFrame, red_kws: str, blue_kws: str, yellow_kws: str):
    """
    사용자가 입력한 키워드에 따라 행 전체에 배경색과 글자색 하이라이트를 적용합니다.
    - 우선순위: 빨강 > 파랑 > 노랑
    - 키워드 매칭은 행 전체의 텍스트를 대상으로 하며, 대소문자를 구분하지 않습니다.
    """
    if not any([red_kws, blue_kws, yellow_kws]):
        return df.style

    r_kws = [kw.strip().lower() for kw in red_kws.split(',') if kw.strip()]
    b_kws = [kw.strip().lower() for kw in blue_kws.split(',') if kw.strip()]
    y_kws = [kw.strip().lower() for kw in yellow_kws.split(',') if kw.strip()]

    # 하이라이트 스타일 정의 (배경색 + 글자색)
    colors = {
        'red': 'background-color: #ffcccc; color: black;',
        'blue': 'background-color: #cce6ff; color: black;',
        'yellow': 'background-color: #ffffcc; color: black;'
    }

    def highlight_logic(row):
        full_row_text = ' '.join(row.astype(str)).lower()
        style = [''] * len(row)

        if r_kws and any(kw in full_row_text for kw in r_kws):
            style = [colors['red']] * len(row)
        elif b_kws and any(kw in full_row_text for kw in b_kws):
            style = [colors['blue']] * len(row)
        elif y_kws and any(kw in full_row_text for kw in y_kws):
            style = [colors['yellow']] * len(row)
            
        return style

    return df.style.apply(highlight_logic, axis=1)
