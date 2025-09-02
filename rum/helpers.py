import streamlit as st
import pandas as pd
import re

def effective_hidden(all_cols: list[str], user_hidden: list[str], hide_defaults: list[str], fixed_pin: str) -> list[str]:
    """
    실제로 숨겨야 할 열 목록을 계산합니다.
    기본 숨김 목록과 사용자가 선택한 숨김 목록을 합치고, 고정 열은 제외합니다.

    Args:
        all_cols: 데이터프레임의 모든 열 목록
        user_hidden: 사용자가 선택한 숨길 열 목록
        hide_defaults: 기본적으로 숨길 열 목록
        fixed_pin: 테이블에 항상 고정되어야 하는 열 이름

    Returns:
        실제로 숨겨질 열들의 정렬된 목록
    """
    # 기본 숨김 목록과 사용자 지정 숨김 목록을 합칩니다.
    hidden = (set(hide_defaults) | set(user_hidden)) & set(all_cols)
    # 고정 핀으로 지정된 열은 숨김 목록에서 제외합니다.
    if fixed_pin in hidden:
        hidden.remove(fixed_pin)
    return sorted(list(hidden))

def sanitize_pin_slots(slot_values: list[str], visible_candidates: list[str], count: int, fixed_pin: str) -> list[str]:
    """
    핀 슬롯 값을 정리하고 유효한 값만 남깁니다.
    중복을 제거하고, 보이는 열 목록에 있는 값만 유지하며, 최대 개수를 맞춥니다.

    Args:
        slot_values: 현재 설정된 핀 슬롯 값 목록
        visible_candidates: 현재 화면에 보이는 열 목록 (핀 후보)
        count: 최대 핀 슬롯 개수
        fixed_pin: 테이블에 항상 고정되어야 하는 열 이름

    Returns:
        정리된 핀 슬롯 값 목록
    """
    allow = set(visible_candidates)
    seen, out = set(), []
    for v in slot_values:
        c = (v or "").strip()
        # 유효한 후보이고, 고정 핀이 아니며, 중복되지 않은 경우에만 추가합니다.
        if c and c in allow and c not in seen and c != fixed_pin:
            out.append(c)
            seen.add(c)
    # 최대 개수에 맞춰 빈 슬롯을 추가합니다.
    out += [""] * (count - len(out))
    return out[:count]

def reorder_for_pinned(df: pd.DataFrame, fixed_second: str, pin_slots: list[str]) -> pd.DataFrame:
    """
    고정 핀 설정에 따라 데이터프레임의 열 순서를 재정렬합니다.

    Args:
        df: 재정렬할 데이터프레임
        fixed_second: 항상 두 번째에 위치할 고정 열 이름
        pin_slots: 사용자가 지정한 고정 열 목록

    Returns:
        열 순서가 재정렬된 데이터프레임
    """
    if df is None or df.empty:
        return df
    
    pins, seen = [], set()
    # 1. 타임스탬프 열을 가장 먼저 추가합니다.
    if "timestamp(KST)" in df.columns:
        pins.append("timestamp(KST)")
        seen.add("timestamp(KST)")
    # 2. 두 번째 고정 열을 추가합니다.
    if fixed_second in df.columns and fixed_second not in seen:
        pins.append(fixed_second)
        seen.add(fixed_second)
    # 3. 사용자가 지정한 핀들을 순서대로 추가합니다.
    for c in pin_slots:
        if c and c in df.columns and c not in seen:
            pins.append(c)
            seen.add(c)
    # 4. 나머지 열들을 추가합니다.
    rest = [c for c in df.columns if c not in seen]
    return df[pins + rest]

def filter_dataframe(df: pd.DataFrame, column: str, filter_text: str, is_and: bool) -> pd.DataFrame:
    """
    주어진 조건에 따라 데이터프레임을 필터링합니다.

    Args:
        df: 필터링할 데이터프레임
        column: 필터링을 적용할 열 이름
        filter_text: 사용자가 입력한 필터 키워드 (쉼표로 구분)
        is_and: AND/OR 조건 여부 (True: AND, False: OR)

    Returns:
        필터링된 데이터프레임
    """
    if column not in df.columns:
        st.warning(f"'{column}' 컬럼이 없어 필터링할 수 없습니다.")
        return df

    # 필터 키워드를 쉼표로 분리하고 공백을 제거합니다.
    keywords = [kw.strip() for kw in filter_text.split(',') if kw.strip()]
    if not keywords:
        return df

    series = df[column].fillna('')
    
    if is_and:
        # AND 조건: 모든 키워드를 포함해야 함
        condition = pd.Series(True, index=df.index)
        for kw in keywords:
            condition &= series.str.contains(re.escape(kw), case=False, regex=True)
    else:
        # OR 조건: 키워드 중 하나라도 포함하면 됨
        regex_pattern = '|'.join(re.escape(kw) for kw in keywords)
        condition = series.str.contains(regex_pattern, case=False, regex=True)
    
    return df[condition]
