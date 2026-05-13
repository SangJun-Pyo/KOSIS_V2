from __future__ import annotations

import streamlit as st


def inject_styles() -> None:
    st.markdown(
        """
<style>
[data-testid="stHeader"] { background: transparent; }
[data-testid="stToolbar"] { display: none !important; }
#MainMenu, footer { visibility: hidden; }
.block-container { max-width: 1680px; padding-top: 14px; padding-bottom: 28px; }
html, body, [class*="css"] { color: #1f2937; }
.muted { color: #6b7280; font-size: 14px; line-height: 1.7; }
.status-chip {
  display: inline-flex; align-items: center; min-height: 28px; padding: 4px 12px;
  border-radius: 999px; border: 1px solid #dbeafe; background: #eff6ff;
  color: #1d4ed8; font-size: 13px; font-weight: 700;
}
.status-chip.ok { background: #ecfdf3; border-color: #bbf7d0; color: #15803d; }
.status-chip.warn { background: #fff7ed; border-color: #fed7aa; color: #c2410c; }
.status-chip.fail { background: #fef2f2; border-color: #fecaca; color: #b91c1c; }
.status-chip.gray { background: #f8fafc; border-color: #e5e7eb; color: #475569; }
.info-box {
  background: #f8fbff; border: 1px solid #dbeafe; border-radius: 12px;
  padding: 14px 16px; color: #1d4ed8; line-height: 1.7;
  word-break: break-word; overflow-wrap: anywhere;
}
.kv-grid { display: grid; grid-template-columns: 140px 1fr; gap: 10px 14px; align-items: start; }
.kv-key { color: #475569; font-size: 13px; font-weight: 700; }
.kv-value { color: #0f172a; font-size: 14px; line-height: 1.7; word-break: break-word; overflow-wrap: anywhere; }
.region-map-card {
  background: linear-gradient(180deg, #0f172a 0%, #0b1120 100%);
  border: 1px solid #1e3a5f;
  border-radius: 18px;
  padding: 14px;
}
.region-map-note {
  color: #cbd5e1;
  font-size: 12px;
  line-height: 1.6;
}
</style>
""",
        unsafe_allow_html=True,
    )

