from __future__ import annotations

import base64
import io
import re
from pathlib import Path
from typing import Any

import requests


def _lang(report_language: str) -> str:
    l = (report_language or "English").strip().lower()
    if l.startswith("it"):
        return "it"
    if l.startswith("es"):
        return "es"
    return "en"


def _t(report_language: str, key: str) -> str:
    txt = {
        "pdf_title": {"en": "Scouting report", "it": "Report scouting", "es": "Informe de scouting"},
        "section_match_context": {"en": "Match context", "it": "Contesto partita", "es": "Contexto del partido"},
        "section_scout_report": {"en": "Scouting report", "it": "Report scout", "es": "Reporte scout"},
        "section_stats_report": {"en": "Statistical report", "it": "Report statistico", "es": "Reporte estadistico"},
        "section_visualizations": {"en": "Visualizations", "it": "Visualizzazioni", "es": "Visualizaciones"},
        "label_match": {"en": "Match", "it": "Partita", "es": "Partido"},
        "label_competition": {"en": "Competition", "it": "Competizione", "es": "Competicion"},
        "label_date": {"en": "Date", "it": "Data", "es": "Fecha"},
        "label_player": {"en": "Player", "it": "Giocatore", "es": "Jugador"},
        "label_team": {"en": "Team", "it": "Squadra", "es": "Equipo"},
        "fallback_scout": {
            "en": "No final report text available.",
            "it": "Nessun testo finale del report disponibile.",
            "es": "No hay texto final del reporte disponible.",
        },
        "fallback_stats": {
            "en": "No statistical summary available.",
            "it": "Nessun riepilogo statistico disponibile.",
            "es": "No hay resumen estadistico disponible.",
        },
        "fallback_no_text": {
            "en": "No textual description available.",
            "it": "Nessuna descrizione testuale disponibile.",
            "es": "No hay descripcion textual disponible.",
        },
        "fallback_no_image": {
            "en": "No image available.",
            "it": "Nessuna immagine disponibile.",
            "es": "No hay imagen disponible.",
        },
        "phase_defensive": {"en": "Defensive phase", "it": "Fase difensiva", "es": "Fase defensiva"},
        "phase_buildup": {"en": "Build-up phase", "it": "Fase di costruzione", "es": "Fase de construccion"},
        "phase_finalization": {"en": "Finalization phase", "it": "Fase di finalizzazione", "es": "Fase de finalizacion"},
        "viz_duels": {"en": "Duels", "it": "Duelli", "es": "Duelos"},
        "viz_recoveries": {"en": "Recoveries & interceptions", "it": "Recuperi e intercetti", "es": "Recuperaciones e intercepciones"},
        "viz_touch_density": {"en": "Touch density", "it": "Densita tocchi", "es": "Densidad de toques"},
        "viz_pass_start": {"en": "Pass start + network", "it": "Origine passaggi + rete", "es": "Inicio de pases + red"},
        "viz_receiving": {"en": "Receiving + network", "it": "Ricezioni + rete", "es": "Recepciones + red"},
        "viz_shot_map": {"en": "Shot map", "it": "Mappa tiri", "es": "Mapa de tiros"},
        "viz_crosses": {"en": "Crosses & key passes", "it": "Cross e passaggi chiave", "es": "Centros y pases clave"},
    }
    return txt.get(key, {}).get(_lang(report_language), txt.get(key, {}).get("en", key))


def _localize_phase_name(report_language: str, phase_name: str) -> str:
    p = (phase_name or "").strip().lower()
    if "defensive" in p or "difens" in p or "defensiva" in p:
        return _t(report_language, "phase_defensive")
    if "build" in p or "costru" in p:
        return _t(report_language, "phase_buildup")
    if "final" in p or "offens" in p:
        return _t(report_language, "phase_finalization")
    return phase_name


def _localize_viz_title(report_language: str, title: str) -> str:
    x = (title or "").strip().lower()
    if "duel" in x:
        return _t(report_language, "viz_duels")
    if "recover" in x or "intercept" in x:
        return _t(report_language, "viz_recoveries")
    if "touch density" in x:
        return _t(report_language, "viz_touch_density")
    if "pass start" in x:
        return _t(report_language, "viz_pass_start")
    if "receiving" in x:
        return _t(report_language, "viz_receiving")
    if "shot" in x:
        return _t(report_language, "viz_shot_map")
    if "cross" in x or "key pass" in x:
        return _t(report_language, "viz_crosses")
    return title


def _require_reportlab():
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import cm
        from reportlab.lib.utils import ImageReader
        from reportlab.platypus import (
        Image,
            PageBreak,
            KeepTogether,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except Exception as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError(
            "PDF export requires 'reportlab'. Install with: pip install reportlab"
        ) from exc
    return {
        "colors": colors,
        "A4": A4,
        "ParagraphStyle": ParagraphStyle,
        "getSampleStyleSheet": getSampleStyleSheet,
        "cm": cm,
        "ImageReader": ImageReader,
        "Image": Image,
        "PageBreak": PageBreak,
        "KeepTogether": KeepTogether,
        "Paragraph": Paragraph,
        "SimpleDocTemplate": SimpleDocTemplate,
        "Spacer": Spacer,
        "Table": Table,
        "TableStyle": TableStyle,
    }


def _decode_data_uri(uri: str) -> bytes | None:
    m = re.match(r"^data:image/[^;]+;base64,(.+)$", (uri or "").strip())
    if not m:
        return None
    try:
        return base64.b64decode(m.group(1))
    except Exception:
        return None


def _load_image_bytes(source: str | None) -> bytes | None:
    src = (source or "").strip()
    if not src:
        return None
    if src.startswith("data:image/"):
        return _decode_data_uri(src)
    if src.startswith("http://") or src.startswith("https://"):
        try:
            r = requests.get(src, timeout=15)
            r.raise_for_status()
            return r.content
        except Exception:
            return None
    p = Path(src)
    if p.is_file():
        try:
            return p.read_bytes()
        except Exception:
            return None
    return None


def _extract_png_from_markdown(md_img: str) -> bytes | None:
    m = re.match(r"^!\[\]\((.+)\)\s*$", (md_img or "").strip())
    if not m:
        return None
    src = m.group(1).strip()
    return _load_image_bytes(src)


def _clean_markdown_for_pdf(text: str) -> str:
    """Convert lightweight markdown-ish content into plain text for PDF paragraphs."""
    t = (text or "").replace("\r\n", "\n")
    # headings / bullets
    t = re.sub(r"(?m)^\s{0,3}#{1,6}\s*", "", t)
    t = re.sub(r"(?m)^\s*[-*]\s+", "• ", t)
    t = re.sub(r"(?m)^\s*\d+\.\s+", "• ", t)
    # emphasis / code
    t = re.sub(r"\*\*(.*?)\*\*", r"\1", t)
    t = re.sub(r"__(.*?)__", r"\1", t)
    t = re.sub(r"`([^`]*)`", r"\1", t)
    # links: [text](url) -> text (url)
    t = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", t)
    return t.strip()


def _clean_inline_markdown(text: str) -> str:
    t = text or ""
    t = re.sub(r"\*\*(.*?)\*\*", r"\1", t)
    t = re.sub(r"__(.*?)__", r"\1", t)
    t = re.sub(r"`([^`]*)`", r"\1", t)
    t = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", t)
    return t.strip()


def _append_markdown_text(
    *,
    story: list[Any],
    text: str,
    body_style: Any,
    subsection_style: Any,
    bullet_style: Any,
    Paragraph: Any,
) -> None:
    source = (text or "").replace("\r\n", "\n").strip()
    if not source:
        return

    lines = source.split("\n")
    paragraph_buf: list[str] = []
    subsection_started = False

    def _flush_paragraph() -> None:
        if paragraph_buf:
            ptxt = " ".join(x.strip() for x in paragraph_buf if x.strip()).strip()
            if ptxt:
                story.append(Paragraph(ptxt, body_style))
            paragraph_buf.clear()

    for raw in lines:
        line = raw.strip()
        if not line:
            _flush_paragraph()
            continue

        # Subsections from markdown headings (## ...), bold-only line, or short "Label:" lines.
        is_md_heading = bool(re.match(r"^\s*#{1,6}\s+", raw))
        is_bold_line = bool(re.match(r"^\s*\*\*[^*]+\*\*\s*$", raw)) or bool(
            re.match(r"^\s*__[^_]+__\s*$", raw)
        )
        is_label_line = line.endswith(":") and len(line) <= 90
        words = re.findall(r"[A-Za-zÀ-ÿ0-9']+", line)
        has_terminal_punct = line.endswith(".") or line.endswith("!") or line.endswith("?")
        is_plain_heading = (
            len(words) >= 2
            and len(words) <= 6
            and not has_terminal_punct
            and not line.startswith("•")
            and not re.match(r"^[-*]\s+", line)
            and not re.match(r"^\d+\.\s+", line)
        )
        if is_md_heading or is_bold_line or is_label_line or is_plain_heading:
            _flush_paragraph()
            title = re.sub(r"^\s*#{1,6}\s*", "", line).rstrip(":").strip()
            title = _clean_inline_markdown(title)
            if title:
                if subsection_started:
                    story.append(Paragraph(" ", body_style))
                story.append(Paragraph(f"<b>{title}</b>", subsection_style))
                subsection_started = True
            continue

        # Bullet / numbered bullet lines.
        if re.match(r"^[-*]\s+", line) or re.match(r"^\d+\.\s+", line) or line.startswith("• "):
            _flush_paragraph()
            item = re.sub(r"^[-*]\s+", "", line)
            item = re.sub(r"^\d+\.\s+", "", item)
            item = re.sub(r"^•\s+", "", item)
            item = _clean_inline_markdown(item)
            story.append(Paragraph(item, bullet_style, bulletText="•"))
            continue

        paragraph_buf.append(_clean_inline_markdown(line))

    _flush_paragraph()


def _img_flowable(raw: bytes, width: float, height: float, R: dict[str, Any]):
    Image = R["Image"]
    img = Image(io.BytesIO(raw))
    img._restrictSize(width, height)
    return img


def build_scouting_report_pdf(
    *,
    logo_path: str,
    match_context: dict[str, Any],
    report_text: str,
    statistical_summary: str,
    phase_viz: list[tuple[str, list[tuple[str, str]]]],
    result: dict[str, Any],
) -> bytes:
    R = _require_reportlab()
    colors = R["colors"]
    A4 = R["A4"]
    getSampleStyleSheet = R["getSampleStyleSheet"]
    ParagraphStyle = R["ParagraphStyle"]
    cm = R["cm"]
    ImageReader = R["ImageReader"]
    Paragraph = R["Paragraph"]
    PageBreak = R["PageBreak"]
    Spacer = R["Spacer"]
    Table = R["Table"]
    TableStyle = R["TableStyle"]
    SimpleDocTemplate = R["SimpleDocTemplate"]

    styles = getSampleStyleSheet()
    h2 = ParagraphStyle(
        "H2",
        parent=styles["Heading2"],
        fontSize=14,
        spaceBefore=10,
        spaceAfter=6,
    )
    h3 = ParagraphStyle(
        "H3",
        parent=styles["Heading3"],
        fontSize=11,
        spaceBefore=8,
        spaceAfter=4,
    )
    body = ParagraphStyle(
        "Body",
        parent=styles["BodyText"],
        fontSize=10,
        leading=14,
        spaceAfter=4,
    )
    caption = ParagraphStyle(
        "Caption",
        parent=styles["BodyText"],
        fontSize=9,
        textColor=colors.HexColor("#555555"),
        leading=12,
    )
    body_compact = ParagraphStyle(
        "BodyCompact",
        parent=body,
        fontSize=9,
        leading=12,
        spaceAfter=2,
    )
    subsection = ParagraphStyle(
        "Subsection",
        parent=styles["Heading4"],
        fontSize=10,
        leading=13,
        spaceBefore=6,
        spaceAfter=3,
        textColor=colors.HexColor("#1f3a5f"),
    )
    bullet = ParagraphStyle(
        "Bullet",
        parent=body,
        leftIndent=10,
        firstLineIndent=0,
        spaceBefore=1,
        spaceAfter=2,
    )

    width, height = A4
    margin = 1.4 * cm
    top_reserved = 2.1 * cm
    bottom_reserved = 1.3 * cm

    buffer = io.BytesIO()
    report_language = str(match_context.get("report_language") or "English")
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=margin,
        rightMargin=margin,
        topMargin=margin + top_reserved,
        bottomMargin=margin + bottom_reserved,
        title=_t(report_language, "pdf_title"),
    )

    logo_raw = None
    try:
        logo_raw = Path(logo_path).read_bytes() if Path(logo_path).is_file() else None
    except Exception:
        logo_raw = None
    player_raw = _load_image_bytes(str(match_context.get("player_image_data_url") or ""))
    team_raw = _load_image_bytes(str(match_context.get("team_image_data_url") or ""))

    footer_text = (
        f"{_t(report_language, 'pdf_title')} - {match_context.get('match_label', '')} - "
        f"{match_context.get('player_name', '')}"
    )

    def _header_footer(canvas, _doc, include_side_images: bool):
        canvas.saveState()
        y_top = height - margin - 0.2 * cm
        if logo_raw:
            try:
                canvas.drawImage(
                    ImageReader(io.BytesIO(logo_raw)),
                    margin,
                    y_top - 1.2 * cm,
                    width=3.2 * cm,
                    height=1.2 * cm,
                    preserveAspectRatio=True,
                    mask="auto",
                )
            except Exception:
                pass
        if include_side_images:
            x_right = width - margin
            if team_raw:
                try:
                    canvas.drawImage(
                        ImageReader(io.BytesIO(team_raw)),
                        x_right - 1.2 * cm,
                        y_top - 1.05 * cm,
                        width=1.0 * cm,
                        height=1.0 * cm,
                        preserveAspectRatio=True,
                        mask="auto",
                    )
                except Exception:
                    pass
            if player_raw:
                try:
                    canvas.drawImage(
                        ImageReader(io.BytesIO(player_raw)),
                        x_right - 2.45 * cm,
                        y_top - 1.05 * cm,
                        width=1.0 * cm,
                        height=1.0 * cm,
                        preserveAspectRatio=True,
                        mask="auto",
                    )
                except Exception:
                    pass

        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#666666"))
        canvas.drawCentredString(width / 2, margin - 0.55 * cm, footer_text[:170])
        canvas.restoreState()

    story: list[Any] = []

    # Match context
    story.append(Paragraph(_t(report_language, "section_match_context"), h2))
    info_html = (
        f"<b>{_t(report_language, 'label_match')}:</b> {match_context.get('match_label', '')}<br/>"
        f"<b>{_t(report_language, 'label_competition')}:</b> {match_context.get('competition_name', '')}<br/>"
        f"<b>{_t(report_language, 'label_date')}:</b> {match_context.get('match_date', '')}<br/>"
        f"<b>{_t(report_language, 'label_player')}:</b> {match_context.get('player_name', '')}<br/>"
        f"<b>{_t(report_language, 'label_team')}:</b> {match_context.get('team_name', '')}"
    )
    context_cells: list[Any] = [Paragraph(info_html, body)]
    context_cells.append(
        _img_flowable(player_raw, 2.2 * cm, 2.2 * cm, R) if player_raw else Paragraph("", body)
    )
    context_cells.append(
        _img_flowable(team_raw, 2.2 * cm, 2.2 * cm, R) if team_raw else Paragraph("", body)
    )
    ctbl = Table([context_cells], colWidths=[doc.width * 0.6, doc.width * 0.2, doc.width * 0.2])
    ctbl.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (1, 0), (-1, 0), "CENTER"),
            ]
        )
    )
    story.extend([ctbl, Spacer(1, 0.3 * cm)])

    # Scout report
    story.append(Paragraph(_t(report_language, "section_scout_report"), h2))
    scout_text = report_text or _t(report_language, "fallback_scout")
    _append_markdown_text(
        story=story,
        text=scout_text,
        body_style=body,
        subsection_style=subsection,
        bullet_style=bullet,
        Paragraph=Paragraph,
    )
    story.append(Spacer(1, 0.2 * cm))

    # Statistical report (new page)
    story.append(PageBreak())
    story.append(Paragraph(_t(report_language, "section_stats_report"), h2))
    stats_text = statistical_summary or _t(report_language, "fallback_stats")
    _append_markdown_text(
        story=story,
        text=stats_text,
        body_style=body,
        subsection_style=subsection,
        bullet_style=bullet,
        Paragraph=Paragraph,
    )
    story.append(Spacer(1, 0.3 * cm))

    # Visualizations (new page)
    story.append(PageBreak())
    story.append(Paragraph(_t(report_language, "section_visualizations"), h2))
    for phase_name, viz_items in phase_viz:
        phase_header = Paragraph(_localize_phase_name(report_language, phase_name), h3)
        row_blocks: list[Any] = []
        for state_key, title in viz_items:
            block = result.get(state_key) if isinstance(result.get(state_key), dict) else {}
            md_img = str(block.get("markdown_image") or "").strip()
            desc = str(block.get("description") or "").strip()
            cap = str(block.get("caption") or title).strip()
            err = str(block.get("error") or "").strip()
            raw_img = _extract_png_from_markdown(md_img) if md_img else None

            # Left: text, Right: image (layout requested).
            desc_short = _clean_markdown_for_pdf(desc)
            if len(desc_short) > 1100:
                desc_short = desc_short[:1100].rstrip() + "..."
            local_title = _localize_viz_title(report_language, title)
            left_parts: list[Any] = [Paragraph(f"<b>{local_title}</b>", body_compact)]
            if cap and cap != title:
                left_parts.append(Paragraph(cap, caption))
            if desc_short:
                for para in [p.strip() for p in desc_short.split("\n\n") if p.strip()]:
                    left_parts.append(Paragraph(para.replace("\n", "<br/>"), body_compact))
            elif err:
                left_parts.append(Paragraph(f"<i>{err}</i>", caption))
            else:
                left_parts.append(Paragraph(_t(report_language, "fallback_no_text"), caption))
            left_tbl = Table([[p] for p in left_parts], colWidths=[doc.width * 0.6 - 0.2 * cm])
            left_tbl.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 0),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                        ("TOPPADDING", (0, 0), (-1, -1), 0),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ]
                )
            )

            right_obj: Any
            if raw_img:
                right_obj = _img_flowable(raw_img, doc.width * 0.4 - 0.2 * cm, 7.0 * cm, R)
            else:
                right_obj = Paragraph(_t(report_language, "fallback_no_image"), caption)

            viz_tbl = Table(
                [[left_tbl, right_obj]],
                colWidths=[doc.width * 0.6, doc.width * 0.4],
            )
            viz_tbl.setStyle(
                TableStyle(
                    [
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 4),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ]
                )
            )

            row_blocks.append(viz_tbl)
            row_blocks.append(Spacer(1, 0.2 * cm))
        story.append(phase_header)
        story.extend(row_blocks)
        story.append(Spacer(1, 0.2 * cm))

    doc.build(
        story,
        onFirstPage=lambda canvas, d: _header_footer(canvas, d, False),
        onLaterPages=lambda canvas, d: _header_footer(canvas, d, True),
    )
    return buffer.getvalue()
