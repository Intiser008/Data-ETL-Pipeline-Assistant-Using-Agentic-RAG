from __future__ import annotations

import json
import os
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List
import uuid

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st


st.set_page_config(page_title="Agentic Analytics Assistant", layout="wide")

API_URL = os.getenv("AGENT_API_URL", "http://localhost:8000/query")
DEFAULT_HISTORY_LENGTH = 20


def _call_agent(prompt: str, session_id: str) -> Dict[str, Any]:
    response = requests.post(
        API_URL,
        json={"prompt": prompt, "session_id": session_id},
        timeout=float(os.getenv("AGENT_API_TIMEOUT", "120")),
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        message = response.text
        try:
            payload = response.json()
            detail = payload.get("detail")
            if isinstance(detail, (list, dict, str)):
                message = json.dumps(detail, indent=2) if not isinstance(detail, str) else detail
        except ValueError:
            pass
        raise requests.HTTPError(message, response=response) from exc
    return response.json()


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _to_excel_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Results")
    return buffer.getvalue()


def _render_context(context: List[str]) -> None:
    if not context:
        st.write("No context snippets were returned for this run.")
        return

    st.markdown("**Retrieved citations**")
    for idx, chunk in enumerate(context, start=1):
        preview = chunk.splitlines()[0][:140] if chunk else ""
        with st.expander(f"Citation [{idx}] {preview}..."):
            st.write(chunk)


def _extract_plan_from_history(history: List[Dict[str, Any]]) -> str | None:
    if not history:
        return None
    for turn in reversed(history):
        if turn.get("role") != "agent":
            continue
        summary = turn.get("summary")
        if summary:
            return summary
        sql = turn.get("sql")
        if sql:
            return f"Generated SQL:\n{sql}"
    return None


def _build_summary_text(payload: Dict[str, Any]) -> str:
    intent = (payload.get("intent") or "").upper()
    attempts = payload.get("attempts", 1)
    repaired = payload.get("repaired", False)
    errors = payload.get("errors", [])
    lines: List[str] = []

    lines.append(f"Intent: {intent or 'unknown'}")
    lines.append(f"Attempts: {attempts} (repaired={repaired})")

    if intent == "SQL":
        rows = payload.get("rows") or []
        columns = payload.get("columns") or []
        limit_enforced = payload.get("limit_enforced")
        lines.append(f"Returned rows: {len(rows)}")
        lines.append(f"Columns: {', '.join(columns) if columns else 'None'}")
        if limit_enforced is not None:
            lines.append(f"Limit enforced: {limit_enforced}")
        sql = payload.get("sql")
        if sql:
            lines.append("Generated SQL:")
            lines.append(sql)
    elif intent == "ETL":
        results = payload.get("results") or []
        if results:
            lines.append("Processed tables:")
            for table in results:
                table_name = table.get("table", "unknown")
                row_count = table.get("row_count")
                loaded_rows = table.get("loaded_rows")
                s3_uri = table.get("s3_uri")
                parts = [f"- {table_name}: {row_count} rows"]
                if loaded_rows is not None:
                    parts.append(f"loaded {loaded_rows}")
                if s3_uri:
                    parts.append(f"s3: {s3_uri}")
                lines.append(", ".join(parts))
        else:
            lines.append("No tables were produced.")
    else:
        lines.append("Payload:")
        lines.append(json.dumps(payload, indent=2))

    if errors:
        lines.append("Warnings:")
        for item in errors:
            lines.append(f"- {item}")

    return "\n".join(lines)


def _extract_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    intent = (payload.get("intent") or "").upper()
    if intent == "SQL":
        rows = payload.get("rows") or []
        columns = payload.get("columns") or []
        return {
            "intent": intent,
            "row_count": len(rows),
            "column_count": len(columns),
            "attempts": payload.get("attempts", 1),
            "repaired": payload.get("repaired", False),
            "limit_enforced": payload.get("limit_enforced"),
        }

    if intent == "ETL":
        results = payload.get("results") or []
        total_rows = sum(item.get("row_count", 0) or 0 for item in results)
        loaded = [item.get("loaded_rows") for item in results if item.get("loaded_rows") is not None]
        total_loaded = sum(loaded) if loaded else None
        return {
            "intent": intent,
            "table_count": len(results),
            "rows_produced": total_rows,
            "rows_loaded": total_loaded,
            "attempts": payload.get("attempts", 1),
            "repaired": payload.get("repaired", False),
        }

    return {"intent": intent or "UNKNOWN"}


def _render_metrics(payload: Dict[str, Any]) -> None:
    metrics = _extract_metrics(payload)
    intent = metrics.pop("intent", "UNKNOWN")
    st.markdown("### Run Metrics")
    cols = st.columns(max(len(metrics), 1))
    for col, (name, value) in zip(cols, metrics.items()):
        with col:
            col.metric(name.replace("_", " ").title(), value if value is not None else "—")
    st.caption(f"Intent: {intent}")


def _render_chart_gallery() -> None:
    gallery = st.session_state.get("chart_gallery", [])
    if not gallery:
        return

    st.markdown("### Dashboard")
    cols = st.columns(2)
    for idx, entry in enumerate(gallery):
        fig_dict = entry.get("figure")
        title = entry.get("title", f"Chart {idx + 1}")
        figure = go.Figure(fig_dict) if fig_dict else None
        with cols[idx % 2]:
            st.markdown(f"**{title}**")
            if figure is not None:
                st.plotly_chart(figure, use_container_width=True)
    if st.button("Clear dashboard", key="clear_dashboard"):
        st.session_state["chart_gallery"] = []
        st.success("Dashboard cleared.")


def _save_report(payload: Dict[str, Any], summary_text: str) -> None:
    reports = st.session_state.setdefault("saved_reports", [])
    entry = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        "intent": payload.get("intent", "unknown"),
        "summary": summary_text,
        "payload": payload,
    }
    reports.append(entry)
    st.session_state["saved_reports"] = reports[-20:]  # keep latest 20 reports


def _render_saved_reports_tab() -> None:
    reports = st.session_state.get("saved_reports", [])
    st.subheader("Saved Reports")
    if not reports:
        st.info("No reports saved yet. Generate a result and click 'Save report' to archive it here.")
        return

    for reverse_idx, report in enumerate(reversed(reports)):
        absolute_idx = len(reports) - reverse_idx - 1
        timestamp = report.get("timestamp", "unknown")
        intent = (report.get("intent") or "unknown").upper()
        header = f"{timestamp} · {intent}"
        with st.expander(header, expanded=reverse_idx == 0):
            summary_text = report.get("summary", "")
            st.markdown("#### Narrative summary")
            st.write(summary_text or "_No summary stored._")

            metrics = _extract_metrics(report.get("payload", {}))
            metrics.pop("intent", None)
            if metrics:
                cols = st.columns(max(len(metrics), 1))
                for column, (name, value) in zip(cols, metrics.items()):
                    with column:
                        column.metric(name.replace("_", " ").title(), value if value is not None else "—")

            filename = f"agent_report_{timestamp.replace(':', '-')}.txt"
            st.download_button(
                "Download summary",
                data=summary_text.encode("utf-8"),
                file_name=filename,
                mime="text/plain",
                key=f"download_report_{absolute_idx}",
            )
            if st.button("Remove report", key=f"remove_report_{absolute_idx}"):
                reports.pop(absolute_idx)
                st.session_state["saved_reports"] = reports
                st.experimental_rerun()


def _conversation_dataframe(conversation: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for turn in conversation:
        rows.append(
            {
                "timestamp": turn.get("timestamp"),
                "role": turn.get("role"),
                "intent": turn.get("intent"),
                "summary": turn.get("summary") or turn.get("prompt") or "",
                "sql": turn.get("sql"),
            }
        )
    return pd.DataFrame(rows)


def _render_audit_tab(session_id: str) -> None:
    st.subheader("Audit & Session Log")
    st.caption(f"Session ID: {session_id}")

    latest = st.session_state.get("latest_response")
    if latest:
        st.markdown("#### Latest run metrics")
        metrics = _extract_metrics(latest)
        metrics_df = pd.DataFrame(
            [{"Metric": name.replace('_', ' ').title(), "Value": value if value is not None else "—"} for name, value in metrics.items() if name != "intent"]
        )
        # Ensure a consistent dtype to avoid Arrow conversion errors when rendering
        try:
            metrics_df["Value"] = metrics_df["Value"].astype(str)
        except Exception:
            pass
        st.table(metrics_df)

    conversation = st.session_state.get("conversation", [])
    if not conversation:
        st.info("No conversation history to display yet.")
        return

    df = _conversation_dataframe(conversation)
    st.markdown("#### Conversation timeline")
    st.dataframe(df, use_container_width=True, height=400)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download log (CSV)",
        data=csv_bytes,
        file_name="agent_session_log.csv",
        mime="text/csv",
    )

def _render_sql_response(payload: Dict[str, Any]) -> None:
    st.subheader("Generated SQL")
    st.code(payload["sql"], language="sql")

    with st.expander("Retrieved context", expanded=False):
        _render_context(payload.get("context", []))

    df = pd.DataFrame(payload.get("rows", []))
    if df.empty:
        if payload.get("no_results_stable"):
            attempts = payload.get("stability_attempts") or 2
            st.info(f"No matching records found (confirmed after {attempts} equivalent attempts).")
        else:
            st.info("The query returned no rows.")
    else:
        _render_table_and_tools(df)


def _render_table_and_tools(df: pd.DataFrame) -> None:
    st.subheader("Result preview")
    st.dataframe(df, use_container_width=True)

    cols = st.columns(3)
    with cols[0]:
        st.metric("Rows", len(df))
    with cols[1]:
        st.metric("Columns", len(df.columns))
    with cols[2]:
        numeric_cols = df.select_dtypes("number").columns
        st.metric("Numeric fields", len(numeric_cols))

    with st.expander("Quick statistics", expanded=False):
        try:
            summary = df.describe(include="all").transpose()
            st.dataframe(summary, use_container_width=True)
        except Exception:  # pragma: no cover - some dtypes cannot be summarised together
            st.write("Statistics unavailable for this result set.")

    csv_bytes = _to_csv_bytes(df)
    st.download_button(
        "Download CSV",
        csv_bytes,
        file_name="agent_results.csv",
        mime="text/csv",
    )
    try:
        excel_bytes = _to_excel_bytes(df)
        st.download_button(
            "Download Excel",
            excel_bytes,
            file_name="agent_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception:  # pragma: no cover - optional dependency
        st.info("Install 'xlsxwriter' to enable Excel exports.")

    _render_chart_builder(df)
    _render_chart_gallery()


def _render_chart_builder(df: pd.DataFrame) -> None:
    numeric_cols = df.select_dtypes("number").columns.tolist()
    if df.empty or not numeric_cols:
        st.sidebar.info("Charts become available when the result set has numeric columns.")
        return

    st.sidebar.header("Visualize results")
    chart_type = st.sidebar.selectbox("Chart type", ["Bar", "Line", "Scatter"], key="chart_type")
    x_axis = st.sidebar.selectbox("X-axis", options=df.columns, key="x_axis")

    if chart_type == "Scatter":
        y_axis = st.sidebar.selectbox("Y-axis", options=numeric_cols, key="y_axis_scatter")
        color = st.sidebar.selectbox("Color", options=[None, *df.columns], key="color_scatter")
        fig = px.scatter(df, x=x_axis, y=y_axis, color=color)
        default_title = f"{chart_type} · {x_axis} vs {y_axis}"
    else:
        y_options = st.sidebar.multiselect(
            "Y-axis columns",
            options=numeric_cols,
            default=numeric_cols[:1],
            key="y_axis_multi",
        )
        if not y_options:
            st.sidebar.warning("Select at least one numeric column for the Y-axis.")
            return
        if chart_type == "Bar":
            fig = px.bar(df, x=x_axis, y=y_options, barmode="group")
        else:
            fig = px.line(df, x=x_axis, y=y_options)
        default_title = f"{chart_type} · {x_axis} vs {', '.join(y_options)}"

    st.plotly_chart(fig, use_container_width=True)

    chart_title = st.sidebar.text_input("Chart title", value=default_title, key="chart_title")
    if st.sidebar.button("Add chart to dashboard", key="add_chart"):
        gallery = st.session_state.setdefault("chart_gallery", [])
        gallery.append({"title": chart_title or default_title, "figure": fig.to_dict()})
        st.session_state["chart_gallery"] = gallery
        st.sidebar.success("Chart added to dashboard.")


def _render_etl_response(payload: Dict[str, Any]) -> None:
    st.subheader("ETL summary")
    summaries = pd.DataFrame(payload.get("results", []))
    if summaries.empty:
        st.info("No ETL tables were produced.")
    else:
        st.dataframe(summaries, use_container_width=True)

    with st.expander("Retrieved context", expanded=False):
        _render_context(payload.get("context", []))


def _render_errors(errors: List[str]) -> None:
    if errors:
        with st.expander("Agent warnings / errors", expanded=False):
            for item in errors:
                st.warning(item)


def _render_history():
    conversation = st.session_state.get("conversation", [])
    if not conversation:
        st.sidebar.info("Run a query to see your history here.")
        return
    st.sidebar.header("Conversation")
    recent = conversation[-DEFAULT_HISTORY_LENGTH:]
    for turn in recent:
        role = turn.get("role")
        if role == "user":
            st.sidebar.markdown(f"**User:** {turn.get('prompt', '')}")
        else:
            label = f"Agent ({turn.get('intent', 'agent')})"
            summary = turn.get("summary") or ""
            sql = turn.get("sql")
            with st.sidebar.expander(label):
                if summary:
                    st.write(summary)
                if sql:
                    st.code(sql, language="sql")


def _render_assistant_tab(session_id: str) -> None:
    st.subheader("Ask a question")

    if st.session_state.pop("reset_prompt_input", False):
        st.session_state["prompt_input"] = ""

    with st.form("agent_form"):
        st.text_area("Ask a question", height=140, key="prompt_input")
        submitted = st.form_submit_button("Run")

    if submitted:
        prompt_clean = st.session_state.get("prompt_input", "").strip()
        if not prompt_clean:
            st.warning("Please enter a prompt before running the assistant.")
        else:
            try:
                payload = _call_agent(prompt_clean, session_id=session_id)
            except requests.RequestException as exc:
                st.error(f"Failed to reach agent API: {exc}")
            else:
                st.session_state["latest_response"] = payload
                st.session_state["session_id"] = payload.get("session_id", session_id)
                st.session_state["conversation"] = payload.get("history", [])
                st.session_state["reset_prompt_input"] = True
                st.session_state["chart_gallery"] = []

    response = st.session_state.get("latest_response")
    if not response:
        st.info("Submit a prompt to see results, charts, and export options.")
        _render_history()
        return

    st.caption(f"Session ID: {st.session_state.get('session_id')}")
    intent = (response.get("intent") or "").upper()
    st.write(f"**Detected intent:** {intent or 'UNKNOWN'}")
    _render_metrics(response)

    plan_text = _extract_plan_from_history(st.session_state.get("conversation", []))
    if plan_text:
        with st.expander("Execution plan", expanded=True):
            st.write(plan_text)

    summary_text = st.session_state.get("latest_summary") or _build_summary_text(response)
    st.session_state["latest_summary"] = summary_text

    st.markdown("### Result narrative")
    st.write(summary_text)

    filename = f"agent_run_{datetime.utcnow().isoformat(timespec='seconds').replace(':', '-')}.txt"
    st.download_button(
        "Download narrative",
        data=summary_text.encode("utf-8"),
        file_name=filename,
        mime="text/plain",
        key="download_summary_current",
    )
    if st.button("Save report", key="save_report_main"):
        _save_report(response, summary_text)
        st.success("Report saved to the Saved Reports tab.")

    if intent == "SQL":
        _render_sql_response(response)
    elif intent == "ETL":
        _render_etl_response(response)
    else:
        st.write(response)

    _render_errors(response.get("errors", []))
    _render_history()

def main() -> None:
    st.title("Agentic Analytics Assistant")
    st.caption("Natural language to insights, across whichever dataset your manifest points to.")

    session_id = st.session_state.setdefault("session_id", str(uuid.uuid4()))
    st.session_state.setdefault("prompt_input", "")
    st.session_state.setdefault("saved_reports", [])
    st.session_state.setdefault("chart_gallery", [])
    st.session_state.setdefault("latest_summary", "")

    assistant_tab, reports_tab, audit_tab = st.tabs(["Assistant", "Saved Reports", "Audit Log"])

    with assistant_tab:
        _render_assistant_tab(session_id)

    with reports_tab:
        _render_saved_reports_tab()

    with audit_tab:
        _render_audit_tab(st.session_state.get("session_id", session_id))


if __name__ == "__main__":
    main()