"""
bot4.py — Bot de Analytics y Reportes
======================================
Genera reportes semanales automáticos cada lunes a las 8am
y permite consultas manuales vía Telegram.

Comandos:
  /reporte                    — reporte general de la semana
  /reporte_cliente Nombre     — reporte específico de un cliente
"""

import threading, time, requests, os
def _keepalive():
    time.sleep(30)
    while True:
        try:
            port = int(os.getenv("PORT", "8000"))
            requests.get(f"http://localhost:{port}/health", timeout=5)
        except: pass
        time.sleep(600)
threading.Thread(target=_keepalive, daemon=True).start()

import os
import logging
from datetime import datetime, timedelta, timezone
from collections import Counter
import anthropic
import threading
import time
import httpx

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from dotenv import load_dotenv
from supabase import create_client
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── Variables de entorno ──────────────────────────────────────────────────────

TELEGRAM_TOKEN    = os.getenv("ANALYTICS_BOT_TOKEN")
DIRECTOR_CHAT_ID  = os.getenv("DIRECTOR_CHAT_ID")          # chat_id del dueño de la agencia
SUPABASE_URL      = os.getenv("SUPABASE_PROJECT_URL", "https://ydggwvpndcazmyvsdbec.supabase.co")
SUPABASE_KEY      = os.getenv("SUPABASE_SECRET_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
FB_ACCESS_TOKEN   = os.getenv("FB_ACCESS_TOKEN", "")
FB_AD_ACCOUNT_ID  = os.getenv("FB_AD_ACCOUNT_ID", "")

# ── Cliente Supabase ──────────────────────────────────────────────────────────

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── FastAPI HTTP server ───────────────────────────────────────────────────────

api = FastAPI(title="agencia-ai-analytics", version="1.0.0")

api.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

_tg_bot = None
_tg_loop = None


@api.get("/health")
def health():
    return {"status": "ok", "service": "agencia-ai-analytics"}


@api.get("/analytics/reporte")
def reporte_endpoint():
    desde = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()[:19] + "Z"
    hasta = datetime.now(timezone.utc).isoformat()[:19] + "Z"

    leads = []
    try:
        r = sb.table("leads").select("estado,ts_creado,canal_preferido,nombre").gte("ts_creado", desde).execute()
        leads = r.data or []
    except Exception as e:
        log.warning(f"leads query: {e}")

    total    = len(leads)
    cerrados = sum(1 for l in leads if l.get("estado") == "cerrado")
    pct      = round(cerrados / total * 100) if total else 0
    revenue  = cerrados * 197

    fb = obtener_metricas_fb(desde, hasta)
    roas_raw = fb.get("roas", [])
    roas_val = roas_raw[0].get("value") if roas_raw else None
    actions  = fb.get("actions", [])
    fb_leads = next((a.get("value") for a in actions if a.get("action_type") == "lead"), None)

    return {
        "periodo":        {"desde": desde[:10], "hasta": hasta[:10]},
        "leads_total":    total,
        "cerrados":       cerrados,
        "conversion_pct": pct,
        "revenue":        revenue,
        "impressions":    fb.get("impressions"),
        "clicks":         fb.get("clicks"),
        "spend":          fb.get("spend"),
        "ctr":            fb.get("ctr"),
        "cpc":            fb.get("cpc"),
        "roas":           roas_val,
        "fb_leads":       fb_leads,
        "metrics":        fb,
    }


@api.post("/analytics/reporte")
async def reporte_post_endpoint(request: Request):
    """Genera reporte y lo envía por Telegram si enviar_telegram=True."""
    import asyncio as _asyncio
    data = {}
    try:
        data = await request.json()
    except Exception:
        pass
    enviar_telegram = data.get("enviar_telegram", False)
    desde = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()[:19] + "Z"
    hasta = datetime.now(timezone.utc).isoformat()[:19] + "Z"
    texto = generar_reporte(desde, hasta, titulo="REPORTE — GENERADO DESDE DASHBOARD")
    if enviar_telegram and DIRECTOR_CHAT_ID and _tg_bot and _tg_loop:
        _asyncio.run_coroutine_threadsafe(
            _tg_bot.send_message(chat_id=DIRECTOR_CHAT_ID, text=texto, parse_mode="Markdown"),
            _tg_loop,
        )
    return {"status": "ok"}


_ANALYTICS_SYSTEM = (
    "Eres el Analytics Bot de la Agencia AI de Roberto. "
    "Especialista en métricas de marketing digital, Facebook Ads, leads, conversión y ROI. "
    "Ayudas a interpretar datos, identificar tendencias y dar recomendaciones accionables. "
    "Responde en español, con datos concretos y análisis directo al punto."
)

@api.post("/analytics/chat")
async def analytics_chat(request: Request):
    """Endpoint para el dashboard — chat directo con Analytics Bot."""
    data = await request.json()
    mensaje = data.get("mensaje", "")
    user_id = data.get("user_id", "dashboard")
    if not mensaje:
        return {"respuesta": ""}
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    try:
        resp = claude_con_retry(client,
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            system=_ANALYTICS_SYSTEM,
            messages=[{"role": "user", "content": mensaje}],
        )
        respuesta = resp.content[0].text
    except Exception as e:
        log.error(f"[CHAT] Error: {e}")
        respuesta = f"Error procesando consulta: {e}"
    return {"respuesta": respuesta}


def _run_api():
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(api, host="0.0.0.0", port=port, log_level="warning")


# ── Retry Claude 529 ──────────────────────────────────────────────────────────

def claude_con_retry(client, **kwargs):
    """Llama a client.messages.create con reintentos exponenciales para error 529."""
    for intento in range(4):
        try:
            return client.messages.create(**kwargs)
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and intento < 3:
                espera = 5 * (2 ** intento)  # 5s, 10s, 20s
                time.sleep(espera)
            else:
                raise

# ── Analytics loop: OBSERVE→LEARN ─────────────────────────────────────────────

def _calcular_metricas(desde: str, hasta: str, cliente=None) -> dict:
    logs  = obtener_logs(desde, hasta, cliente)
    proy  = obtener_proyectos(desde, hasta, cliente)
    cont  = obtener_contenidos(desde, hasta, cliente)
    total_logs = len(logs)
    total_cont = len(cont)
    errores    = sum(1 for l in logs if l.get("tipo") == "error")
    aprobados  = sum(1 for c in cont if c.get("status") == "aprobado")
    return {
        "tasa_error":            round(errores / total_logs * 100) if total_logs else 0,
        "total_proyectos":       len(proy),
        "proyectos_completados": sum(1 for p in proy if p.get("estado") == "completado"),
        "tasa_aprobacion":       round(aprobados / total_cont * 100) if total_cont else 0,
        "total_logs":            total_logs,
    }

def analizar_y_guardar(metricas: dict, periodo: str, cliente=None):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = (
            f"Analiza estas métricas de la agencia de marketing IA y genera un insight en 2-3 líneas:\n"
            f"Período: {periodo}\n"
            f"Tasa de error: {metricas.get('tasa_error', 0)}%\n"
            f"Proyectos completados: {metricas.get('proyectos_completados', 0)}/{metricas.get('total_proyectos', 0)}\n"
            f"Tasa de aprobación de contenidos: {metricas.get('tasa_aprobacion', 0)}%\n"
            f"Total eventos de bots: {metricas.get('total_logs', 0)}\n\n"
            f"Responde solo con el insight, sin encabezados."
        )
        resp = claude_con_retry(client,
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}]
        )
        insight = resp.content[0].text.strip()
        tasa_error = metricas.get("tasa_error", 0)
        score = 8 if tasa_error < 10 else (6 if tasa_error < 20 else 4)
        sb.table("specialist_memory").insert({
            "agent_id":    "analytics",
            "memory_type": "performance_report",
            "content": {
                "insight":        insight,
                "metricas_clave": metricas,
                "periodo":        periodo,
                "cliente":        cliente or "general",
            },
            "score":       score,
            "cliente":     cliente or None,
            "context_key": cliente or "general",
            "tags":        ["analytics", "reporte"] + ([cliente] if cliente else []),
        }).execute()
    except Exception:
        pass

# ── Helpers de fecha ──────────────────────────────────────────────────────────

def semana_anterior() -> tuple[str, str]:
    """Retorna (inicio_iso, fin_iso) de la semana anterior (lunes a domingo)."""
    hoy = datetime.now(timezone.utc).date()
    # Lunes de esta semana
    lunes_esta = hoy - timedelta(days=hoy.weekday())
    # Lunes y domingo de la semana anterior
    lunes_ant  = lunes_esta - timedelta(days=7)
    domingo_ant = lunes_esta - timedelta(days=1)
    return (
        lunes_ant.isoformat() + "T00:00:00Z",
        domingo_ant.isoformat() + "T23:59:59Z",
    )

def semana_actual() -> tuple[str, str]:
    """Retorna (inicio_iso, fin_iso) de la semana actual."""
    hoy = datetime.now(timezone.utc)
    lunes = hoy - timedelta(days=hoy.weekday())
    lunes_str = lunes.date().isoformat() + "T00:00:00Z"
    fin_str   = hoy.isoformat()[:19] + "Z"
    return lunes_str, fin_str

# ── Funciones de consulta a Supabase ─────────────────────────────────────────

def obtener_proyectos(desde: str, hasta: str, cliente: str = None) -> list:
    """Obtiene proyectos_agencia en el rango de fechas dado."""
    try:
        query = (
            sb.table("proyectos_agencia")
            .select("*")
            .gte("ts_creado", desde)
            .lte("ts_creado", hasta)
        )
        if cliente:
            query = query.ilike("cliente", f"%{cliente}%")
        return query.execute().data or []
    except Exception as e:
        log.error(f"Error obteniendo proyectos: {e}")
        return []


def obtener_logs(desde: str, hasta: str, cliente: str = None) -> list:
    """Obtiene log_bots en el rango de fechas dado."""
    try:
        query = (
            sb.table("log_bots")
            .select("*")
            .gte("ts", desde)
            .lte("ts", hasta)
        )
        return query.execute().data or []
    except Exception as e:
        log.error(f"Error obteniendo logs: {e}")
        return []


def obtener_contenidos(desde: str, hasta: str, cliente: str = None) -> list:
    """Obtiene contenidos generados en el rango de fechas dado."""
    try:
        query = (
            sb.table("contenidos")
            .select("*")
            .gte("created_at", desde)
            .lte("created_at", hasta)
        )
        if cliente:
            query = query.ilike("cliente", f"%{cliente}%")
        return query.execute().data or []
    except Exception as e:
        log.error(f"Error obteniendo contenidos: {e}")
        return []


def obtener_usuarios_activos(desde: str, hasta: str) -> int:
    """Cuenta usuarios únicos con actividad en memoria_usuarios."""
    try:
        res = (
            sb.table("memoria_usuarios")
            .select("user_id")
            .gte("updated_at", desde)
            .lte("updated_at", hasta)
            .execute()
        )
        return len(res.data or [])
    except Exception as e:
        log.error(f"Error obteniendo usuarios: {e}")
        return 0

def obtener_metricas_fb(desde: str, hasta: str) -> dict:
    """Consulta Facebook Ads Graph API. Retorna {} si no hay token o si falla."""
    if not FB_ACCESS_TOKEN or not FB_AD_ACCOUNT_ID:
        return {}
    try:
        url    = f"https://graph.facebook.com/v19.0/act_{FB_AD_ACCOUNT_ID}/insights"
        params = {
            "access_token": FB_ACCESS_TOKEN,
            "time_range":   f'{{"since":"{desde[:10]}","until":"{hasta[:10]}"}}',
            "fields":       "spend,impressions,reach,clicks,ctr,cpc,cpp,actions,action_values,roas",
            "level":        "account",
        }
        with httpx.Client(timeout=15) as http:
            r = http.get(url, params=params)
            r.raise_for_status()
            data = r.json().get("data", [])
            return data[0] if data else {}
    except Exception as e:
        log.warning(f"[FB_ADS] {e}")
        return {}


# ── Motor de reportes ─────────────────────────────────────────────────────────

def generar_reporte(desde: str, hasta: str, cliente: str = None, titulo: str = "REPORTE SEMANAL") -> str:
    """
    Genera el texto del reporte con los datos del período dado.
    Si se pasa cliente, filtra por ese cliente.
    """
    proyectos  = obtener_proyectos(desde, hasta, cliente)
    logs       = obtener_logs(desde, hasta, cliente)
    contenidos = obtener_contenidos(desde, hasta, cliente)

    # ── Métricas de proyectos ─────────────────────────────────────────────────
    total_proyectos   = len(proyectos)
    completados       = [p for p in proyectos if p.get("estado") == "completado"]
    fallidos          = [p for p in proyectos if p.get("estado") == "fallido"]
    en_ejecucion      = [p for p in proyectos if p.get("estado") in ("ejecutando", "planificando")]

    # Tasa de éxito de pasos
    todos_pasos = []
    for p in proyectos:
        todos_pasos.extend(p.get("pasos", []))
    pasos_completados = sum(1 for paso in todos_pasos if paso.get("estado") == "completado")
    pasos_fallidos    = sum(1 for paso in todos_pasos if paso.get("estado") == "fallido")
    pasos_total       = len(todos_pasos)
    tasa_exito_pasos  = round(pasos_completados / pasos_total * 100) if pasos_total else 0

    # Bot con más pasos asignados
    bots_pasos = Counter(p.get("bot_responsable", "?") for p in todos_pasos)
    bot_mas_activo_proyectos = bots_pasos.most_common(1)[0] if bots_pasos else ("—", 0)
    # ^^ usado más abajo en la sección de pasos de proyectos

    # Tiempo promedio de proyecto completado (ts_creado → ts_actualizado)
    tiempos = []
    for p in completados:
        try:
            inicio = datetime.fromisoformat(p["ts_creado"].replace("Z", "+00:00"))
            fin    = datetime.fromisoformat(p["ts_actualizado"].replace("Z", "+00:00"))
            tiempos.append((fin - inicio).total_seconds() / 60)  # minutos
        except Exception:
            pass
    tiempo_promedio = round(sum(tiempos) / len(tiempos)) if tiempos else None

    # ── Métricas de logs ──────────────────────────────────────────────────────
    total_logs      = len(logs)
    delegaciones    = [l for l in logs if l.get("tipo") == "delegacion"]
    callbacks       = [l for l in logs if l.get("tipo") == "callback"]
    errores         = [l for l in logs if l.get("tipo") == "error"]
    reintentos      = [l for l in logs if l.get("tipo") == "retry"]
    tasa_error      = round(len(errores) / total_logs * 100) if total_logs else 0

    # Bot con más actividad en logs
    bots_origen = Counter(l.get("bot_origen", "?") for l in logs)
    bot_mas_activo_logs = bots_origen.most_common(1)[0] if bots_origen else ("—", 0)

    # Tipos de tarea más frecuentes
    tipos_tarea = Counter()
    for l in logs:
        payload = l.get("payload") or {}
        if isinstance(payload, dict):
            tipo = payload.get("tipo", "")
            if tipo:
                tipos_tarea[tipo] += 1
    top_tipos = tipos_tarea.most_common(5)

    # Cliente con más trabajo
    clientes_logs = Counter()
    for l in logs:
        payload = l.get("payload") or {}
        if isinstance(payload, dict):
            cli = payload.get("cliente", "")
            if cli:
                clientes_logs[cli] += 1
    for p in proyectos:
        cli = p.get("cliente", "")
        if cli:
            clientes_logs[cli] += 1
    cliente_top = clientes_logs.most_common(1)[0] if clientes_logs else ("—", 0)

    # ── Métricas de contenidos ────────────────────────────────────────────────
    total_contenidos  = len(contenidos)
    aprobados         = sum(1 for c in contenidos if c.get("status") == "aprobado")
    rechazados        = sum(1 for c in contenidos if c.get("status") == "rechazado")
    pendientes_cont   = sum(1 for c in contenidos if c.get("status") == "pendiente")
    tasa_aprobacion   = round(aprobados / total_contenidos * 100) if total_contenidos else 0

    # Bots que generaron contenido
    bots_contenido = Counter(c.get("bot", "?") for c in contenidos)

    # ── Construir texto del reporte ───────────────────────────────────────────
    fecha_desde = desde[:10]
    fecha_hasta = hasta[:10]
    ahora       = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")

    lineas = [
        f"📊 *{titulo}*",
        f"📅 Período: {fecha_desde} → {fecha_hasta}",
        f"🕐 Generado: {ahora}",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━",
        "🚀 *PROYECTOS*",
        f"  • Total: {total_proyectos}",
        f"  • ✅ Completados: {len(completados)}",
        f"  • ❌ Fallidos: {len(fallidos)}",
        f"  • ⏳ En ejecución: {len(en_ejecucion)}",
    ]

    if tiempo_promedio is not None:
        if tiempo_promedio < 60:
            lineas.append(f"  • ⏱ Tiempo promedio: {tiempo_promedio} min")
        else:
            lineas.append(f"  • ⏱ Tiempo promedio: {round(tiempo_promedio/60, 1)} hrs")

    if pasos_total:
        lineas += [
            "",
            "🔧 *PASOS DE PROYECTOS*",
            f"  • Total pasos: {pasos_total}",
            f"  • ✅ Completados: {pasos_completados}",
            f"  • ❌ Fallidos: {pasos_fallidos}",
            f"  • 🎯 Tasa de éxito: {tasa_exito_pasos}%",
        ]
        if bot_mas_activo_proyectos[0] != "—":
            lineas.append(f"  • 🤖 Bot con más pasos: {bot_mas_activo_proyectos[0]} ({bot_mas_activo_proyectos[1]} pasos)")

    lineas += [
        "",
        "🤖 *ACTIVIDAD DE BOTS*",
        f"  • Total eventos: {total_logs}",
        f"  • 📤 Delegaciones: {len(delegaciones)}",
        f"  • 📥 Callbacks: {len(callbacks)}",
        f"  • ⚠️ Errores: {len(errores)}",
        f"  • 🔄 Reintentos: {len(reintentos)}",
        f"  • 🚨 Tasa de error: {tasa_error}%",
    ]

    if bot_mas_activo_logs[0] != "—":
        lineas.append(f"  • 🏆 Bot más activo: {bot_mas_activo_logs[0]} ({bot_mas_activo_logs[1]} eventos)")

    if top_tipos:
        lineas += ["", "📋 *TIPOS DE TAREA MÁS FRECUENTES*"]
        for tipo, count in top_tipos:
            lineas.append(f"  • {tipo}: {count}")

    if not cliente:
        # Solo mostrar top cliente en reporte general
        lineas += [
            "",
            "👥 *CLIENTES*",
            f"  • 🏆 Más activo: {cliente_top[0]} ({cliente_top[1]} tareas)",
        ]
        if clientes_logs:
            lineas.append(f"  • Total clientes con actividad: {len(clientes_logs)}")

    if total_contenidos:
        lineas += [
            "",
            "🎨 *CONTENIDOS GENERADOS*",
            f"  • Total: {total_contenidos}",
            f"  • ✅ Aprobados: {aprobados} ({tasa_aprobacion}%)",
            f"  • ❌ Rechazados: {rechazados}",
            f"  • ⏳ Pendientes: {pendientes_cont}",
        ]
        if bots_contenido:
            bots_str = ", ".join(f"{b}:{n}" for b, n in bots_contenido.most_common(3))
            lineas.append(f"  • Por bot: {bots_str}")

    # Resumen ejecutivo
    lineas += ["", "━━━━━━━━━━━━━━━━━━━━━━━", "💡 *RESUMEN EJECUTIVO*"]
    if total_proyectos == 0 and total_logs == 0:
        lineas.append("  Sin actividad registrada en este período.")
    else:
        if len(completados) > 0:
            lineas.append(f"  ✅ Se completaron {len(completados)} proyecto(s).")
        if tasa_error > 20:
            lineas.append(f"  ⚠️ Tasa de error alta ({tasa_error}%) — revisar logs.")
        elif tasa_error == 0 and total_logs > 0:
            lineas.append(f"  🟢 Sin errores esta semana.")
        if tasa_aprobacion >= 70 and total_contenidos > 0:
            lineas.append(f"  🎯 Alta tasa de aprobación de contenidos ({tasa_aprobacion}%).")
        elif tasa_aprobacion < 50 and total_contenidos > 3:
            lineas.append(f"  📝 Tasa de aprobación baja ({tasa_aprobacion}%) — ajustar estilo.")

    # ── Sección Facebook Ads (solo si hay token configurado) ──────────────────
    fb = obtener_metricas_fb(desde, hasta)
    if fb:
        spend    = fb.get("spend", "—")
        impress  = fb.get("impressions", "—")
        reach    = fb.get("reach", "—")
        clicks   = fb.get("clicks", "—")
        ctr      = fb.get("ctr", "—")
        cpc      = fb.get("cpc", "—")
        roas_raw = fb.get("roas", [])
        roas     = roas_raw[0].get("value", "—") if roas_raw else "—"
        actions  = fb.get("actions", [])
        leads    = next((a.get("value", "0") for a in actions if a.get("action_type") == "lead"), "0")
        lineas += [
            "",
            "━━━━━━━━━━━━━━━━━━━━━━━",
            "📣 *FACEBOOK ADS*",
            f"  • 💰 Gasto: ${spend}",
            f"  • 👁 Impresiones: {impress}",
            f"  • 👥 Alcance: {reach}",
            f"  • 🖱 Clicks: {clicks}",
            f"  • 📊 CTR: {ctr}%",
            f"  • 💵 CPC: ${cpc}",
            f"  • 🎯 Leads: {leads}",
            f"  • 📈 ROAS: {roas}",
        ]

    lineas.append("")
    lineas.append("_Agencia AI — Bot 4 Analytics_ 🤖")

    return "\n".join(lineas)

# ── LOOP AGÉNTICO ANALYTICS ──────────────────────────────────
import datetime as _dt

ANALYTICS_TOOLS = [
    {"name": "calcular_metricas",
     "description": "Calcula métricas de la agencia para un período: proyectos, conversiones, engagement.",
     "input_schema": {"type": "object",
                      "properties": {"desde": {"type": "string"}, "hasta": {"type": "string"},
                                     "cliente": {"type": "string", "default": ""}}}},
    {"name": "obtener_proyectos",
     "description": "Lista proyectos activos y completados con estado y métricas.",
     "input_schema": {"type": "object",
                      "properties": {"desde": {"type": "string"}, "hasta": {"type": "string"},
                                     "cliente": {"type": "string", "default": ""}}}},
    {"name": "metricas_facebook",
     "description": "Métricas de Facebook/Instagram: alcance, engagement, seguidores.",
     "input_schema": {"type": "object",
                      "properties": {"desde": {"type": "string"}, "hasta": {"type": "string"}}}},
    {"name": "obtener_logs",
     "description": "Logs de actividad de los bots para detectar errores o patrones.",
     "input_schema": {"type": "object",
                      "properties": {"desde": {"type": "string"}, "hasta": {"type": "string"},
                                     "cliente": {"type": "string", "default": ""}}}},
    {"name": "generar_reporte_completo",
     "description": "Reporte narrativo con métricas, análisis, insights y recomendaciones.",
     "input_schema": {"type": "object",
                      "properties": {"desde": {"type": "string"}, "hasta": {"type": "string"},
                                     "cliente": {"type": "string", "default": ""},
                                     "titulo": {"type": "string", "default": "REPORTE SEMANAL"}}}}
]

ANALYTICS_SYSTEM = (
    "Eres el Analista de Agencia AI. Transformas datos en decisiones accionables. "
    "Flujo: 1) Calcula métricas del período, 2) Obtén proyectos y redes, "
    "3) Identifica patrones y anomalías, 4) Recomienda acciones priorizadas. "
    "Eres directo: qué funciona, qué no, qué hacer exactamente."
)

def _fechas_default_analytics() -> tuple:
    hoy = _dt.date.today()
    return (hoy - _dt.timedelta(days=7)).strftime("%Y-%m-%d"), hoy.strftime("%Y-%m-%d")

def _tool_analytics(name: str, inp: dict) -> str:
    _d, _h  = _fechas_default_analytics()
    desde   = inp.get("desde", _d)
    hasta   = inp.get("hasta", _h)
    cliente = inp.get("cliente") or None
    try:
        if name == "calcular_metricas":
            return json.dumps(_calcular_metricas(desde, hasta, cliente), ensure_ascii=False, default=str)
        elif name == "obtener_proyectos":
            return json.dumps(obtener_proyectos(desde, hasta, cliente), ensure_ascii=False, default=str)
        elif name == "metricas_facebook":
            return json.dumps(obtener_metricas_fb(desde, hasta), ensure_ascii=False, default=str)
        elif name == "obtener_logs":
            return json.dumps(obtener_logs(desde, hasta, cliente)[:50], ensure_ascii=False, default=str)
        elif name == "generar_reporte_completo":
            return generar_reporte(desde, hasta, cliente, inp.get("titulo", "REPORTE SEMANAL"))
        return f"Tool desconocida: {name}"
    except Exception as e:
        return f"Error en {name}: {e}"

async def loop_agentico_analytics(instruccion: str) -> str:
    messages = [{"role": "user", "content": instruccion}]
    for _ in range(10):
        resp = claude_con_retry(claude, model="claude-haiku-4-5-20251001", max_tokens=4096,
                                system=ANALYTICS_SYSTEM, tools=ANALYTICS_TOOLS, messages=messages)
        if resp.stop_reason == "end_turn":
            return "".join(b.text for b in resp.content if hasattr(b, "text"))
        if resp.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": resp.content})
            tool_results = [{"type": "tool_result", "tool_use_id": b.id,
                             "content": _tool_analytics(b.name, b.input)}
                            for b in resp.content if b.type == "tool_use"]
            messages.append({"role": "user", "content": tool_results})
    return "Análisis completado."

@api.post("/analytics/task")
async def analytics_task(request: Request):
    body         = await request.json()
    instruccion  = body.get("instruccion", body.get("tarea", ""))
    resultado    = await loop_agentico_analytics(instruccion)
    callback_url = body.get("callback_url")
    if callback_url:
        async with httpx.AsyncClient(timeout=10) as c:
            await c.post(callback_url,
                         json={"job_id": body.get("job_id"), "resultado": resultado},
                         headers={"x-callback-secret": body.get("callback_secret", "")})
    return {"status": "ok", "resultado": resultado}
# ── FIN LOOP AGÉNTICO ANALYTICS ──────────────────────────────

# ── Comandos de Telegram ──────────────────────────────────────────────────────

async def cmd_reporte(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera y envía el reporte de la semana actual."""
    await update.message.reply_text("⏳ Generando reporte...")
    desde, hasta = semana_actual()
    texto = generar_reporte(desde, hasta, titulo="REPORTE SEMANA ACTUAL")
    await update.message.reply_text(texto, parse_mode="Markdown")
    metricas = _calcular_metricas(desde, hasta)
    threading.Thread(target=analizar_y_guardar, args=(metricas, f"{desde[:10]}→{hasta[:10]}"), daemon=True).start()


async def cmd_reporte_cliente(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera reporte específico de un cliente: /reporte_cliente NombreCliente"""
    if not context.args:
        await update.message.reply_text(
            "⚠️ Uso: /reporte\\_cliente NombreCliente\n"
            "Ejemplo: /reporte\\_cliente Arranca Financial"
        )
        return

    cliente = " ".join(context.args)
    await update.message.reply_text(f"⏳ Generando reporte para *{cliente}*...", parse_mode="Markdown")
    desde, hasta = semana_actual()
    texto = generar_reporte(desde, hasta, cliente=cliente, titulo=f"REPORTE — {cliente.upper()}")
    await update.message.reply_text(texto, parse_mode="Markdown")
    metricas = _calcular_metricas(desde, hasta, cliente)
    threading.Thread(target=analizar_y_guardar, args=(metricas, f"{desde[:10]}→{hasta[:10]}", cliente), daemon=True).start()


async def cmd_fb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra métricas FB Ads de la semana actual: /fb"""
    await update.message.reply_text("📣 Consultando Facebook Ads...")
    desde, hasta = semana_actual()
    fb = obtener_metricas_fb(desde, hasta)
    if not fb:
        await update.message.reply_text(
            "⚠️ Sin datos de Facebook Ads.\n"
            "Verifica que FB\\_ACCESS\\_TOKEN y FB\\_AD\\_ACCOUNT\\_ID estén configurados.",
            parse_mode="Markdown"
        )
        return
    spend    = fb.get("spend", "—")
    impress  = fb.get("impressions", "—")
    reach    = fb.get("reach", "—")
    clicks   = fb.get("clicks", "—")
    ctr      = fb.get("ctr", "—")
    cpc      = fb.get("cpc", "—")
    roas_raw = fb.get("roas", [])
    roas     = roas_raw[0].get("value", "—") if roas_raw else "—"
    actions  = fb.get("actions", [])
    leads    = next((a.get("value", "0") for a in actions if a.get("action_type") == "lead"), "0")
    texto = (
        f"📣 *FACEBOOK ADS — Semana actual*\n"
        f"📅 {desde[:10]} → {hasta[:10]}\n\n"
        f"💰 Gasto: ${spend}\n"
        f"👁 Impresiones: {impress}\n"
        f"👥 Alcance: {reach}\n"
        f"🖱 Clicks: {clicks}\n"
        f"📊 CTR: {ctr}%\n"
        f"💵 CPC: ${cpc}\n"
        f"🎯 Leads: {leads}\n"
        f"📈 ROAS: {roas}"
    )
    await update.message.reply_text(texto, parse_mode="Markdown")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mensaje de bienvenida."""
    await update.message.reply_text(
        "📊 *Bot de Analytics — Agencia AI*\n\n"
        "Comandos disponibles:\n"
        "• /reporte — Reporte de la semana actual\n"
        "• /reporte\\_cliente Nombre — Reporte por cliente\n\n"
        "El reporte semanal se envía automáticamente cada lunes a las 8am.",
        parse_mode="Markdown"
    )

# ── Tarea programada ──────────────────────────────────────────────────────────

async def enviar_reporte_semanal(app: Application):
    """Se ejecuta cada lunes a las 8am — envía el reporte de la semana anterior."""
    if not DIRECTOR_CHAT_ID:
        log.warning("DIRECTOR_CHAT_ID no configurado — no se puede enviar reporte automático")
        return

    log.info("Generando reporte semanal automático...")
    desde, hasta = semana_anterior()
    texto = generar_reporte(desde, hasta, titulo="REPORTE SEMANAL AUTOMÁTICO")

    try:
        await app.bot.send_message(
            chat_id=DIRECTOR_CHAT_ID,
            text=texto,
            parse_mode="Markdown"
        )
        log.info("Reporte semanal enviado exitosamente.")
        metricas = _calcular_metricas(desde, hasta)
        threading.Thread(target=analizar_y_guardar, args=(metricas, f"{desde[:10]}→{hasta[:10]}"), daemon=True).start()
    except Exception as e:
        log.error(f"Error enviando reporte semanal: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────

async def post_init(app: Application):
    """
    Se ejecuta dentro del event loop de PTB — aquí iniciamos el scheduler
    para garantizar que comparte el mismo event loop y puede llamar
    a app.bot.send_message() sin conflictos.
    """
    global _tg_bot, _tg_loop
    import asyncio as _asyncio
    _tg_bot = app.bot
    _tg_loop = _asyncio.get_event_loop()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        enviar_reporte_semanal,
        trigger=CronTrigger(day_of_week="mon", hour=8, minute=0),
        args=[app],
        id="reporte_semanal",
        name="Reporte semanal automático",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler iniciado — reporte semanal cada lunes 8:00 UTC")


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("ANALYTICS_BOT_TOKEN no configurado en variables de entorno.")

    threading.Thread(target=_run_api, daemon=True).start()

    # Construir la aplicación — post_init arranca el scheduler en el mismo event loop
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )

    # Registrar comandos
    app.add_handler(CommandHandler("start",            cmd_start))
    app.add_handler(CommandHandler("reporte",          cmd_reporte))
    app.add_handler(CommandHandler("reporte_cliente",  cmd_reporte_cliente))
    app.add_handler(CommandHandler("fb",               cmd_fb))

    # Iniciar el bot (polling)
    log.info("Bot 4 Analytics iniciado.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
