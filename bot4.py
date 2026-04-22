"""
bot4.py — Bot de Analytics y Reportes
======================================
Genera reportes semanales automáticos cada lunes a las 8am
y permite consultas manuales vía Telegram.

Comandos:
  /reporte                    — reporte general de la semana
  /reporte_cliente Nombre     — reporte específico de un cliente
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from collections import Counter

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

# ── Cliente Supabase ──────────────────────────────────────────────────────────

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

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

    lineas.append("")
    lineas.append("_Agencia AI — Bot 4 Analytics_ 🤖")

    return "\n".join(lineas)

# ── Comandos de Telegram ──────────────────────────────────────────────────────

async def cmd_reporte(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Genera y envía el reporte de la semana actual."""
    await update.message.reply_text("⏳ Generando reporte...")
    desde, hasta = semana_actual()
    texto = generar_reporte(desde, hasta, titulo="REPORTE SEMANA ACTUAL")
    await update.message.reply_text(texto, parse_mode="Markdown")


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
    except Exception as e:
        log.error(f"Error enviando reporte semanal: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("ANALYTICS_BOT_TOKEN no configurado en variables de entorno.")

    # Construir la aplicación de Telegram
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Registrar comandos
    app.add_handler(CommandHandler("start",            cmd_start))
    app.add_handler(CommandHandler("reporte",          cmd_reporte))
    app.add_handler(CommandHandler("reporte_cliente",  cmd_reporte_cliente))

    # Programar reporte automático — cada lunes a las 8:00am UTC
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

    # Iniciar el bot (polling)
    log.info("Bot 4 Analytics iniciado.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
