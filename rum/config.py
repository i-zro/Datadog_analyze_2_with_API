import os
import streamlit as st
from dataclasses import dataclass

@dataclass
class Settings:
    api_key: str
    app_key: str
    site: str  # 예: "ap1.datadoghq.com" / "datadoghq.com" / "eu.datadoghq.com"

def get_settings() -> Settings:
    api = st.secrets.get("DD_API_KEY", os.getenv("DD_API_KEY", ""))
    app = st.secrets.get("DD_APP_KEY", os.getenv("DD_APP_KEY", ""))
    site = st.secrets.get("DD_SITE", os.getenv("DD_SITE", "datadoghq.com"))
    return Settings(api_key=api, app_key=app, site=site)

def get_api_base(site: str) -> str:
    return f"https://api.{site}"

def get_search_url(site: str) -> str:
    return f"{get_api_base(site)}/api/v2/rum/events/search"

# ─────────────────────────────────────────
# 기본 숨김 컬럼 목록 (사이드바에 노출하지 않음)
# ─────────────────────────────────────────
DEFAULT_HIDDEN_COLUMNS = [
    "session.id","usr.id","attribute.os.build",
    "type","application.id","session.type","view.url","view.referrer",
    "usr.name","usr.email","action.type","action.target.name",
    "resource.type","resource.url","error.message","error.source","error.stack",
    "device.type","os.name","browser.name",
    "attributes.os.build","attributes.os.name","attributes.os.version_major",
    "attributes.resource.size","attributes.resource.method",
    "attributes.resource.provider.domain","attributes.resource.provider.name","attributes.resource.provider.type",
    "attributes.resource.type","attributes.resource.id","attributes.resource.url",
    "attributes.resource.url_host","attributes.resource.url_scheme","attributes.resource.url_path_group",
    "attributes.session.matching_retention_filter.name","attributes.session.matching_retention_filter.id",
    "attributes.session.type","attributes.session.plan","attributes.type",
    "attributes.geo.continent","attributes.geo.country","attributes.geo.as.domain","attributes.geo.as.name","attributes.geo.as.type",
    "attributes.geo.country_iso_code","attributes.geo.city","attributes.geo.latitude","attributes.geo.continent_code",
    "attributes.geo.subdivision_iso_code","attributes.geo.country_subdivision_iso_code","attributes.geo.location",
    "attributes.geo.country_subdivision","attributes.geo.longitude",
    "attributes.view.url_path_group","attributes.view.id","attributes.view.url","attributes.view.url_path",
    "attributes.application.name","attributes.application.short_name","attributes.application.id",
    "attributes.connectivity.cellular.carrier_name","attributes.connectivity.status",
    "attributes.usr.anonymous_id","attributes.usr.usr_id","attributes.service",
    "attributes.device.name",
    "attributes.device.model",
    "attributes.device.type",
    "attributes.device.brand",
    "attributes.device.architecture",
    "attributes._dd.format_version",
    "tags",
    "attributes.usr.id",
    "timestamp",

]

def get_default_hidden_columns() -> list[str]:
    # 복사본 반환(외부에서 수정 방지)
    return list(DEFAULT_HIDDEN_COLUMNS)
