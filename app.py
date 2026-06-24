"""
Vinted Bot — Serveur principal
Lance avec : python app.py
Puis ouvre http://localhost:5000
"""

import asyncio
import sqlite3
import json
import time
import random
import logging
import threading
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from flask import Flask, render_template, jsonify, request, Response
import httpx

app = Flask(__name__)
DB_PATH = Path("data/vinted.db")
DB_PATH.parent.mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# ── State global du bot ──────────────────────────────────────────────────────
bot_state = {
    "running": False,
    "thread": None,
    "last_scan": None,
    "scan_count": 0,
    "new_today": 0,
    "logs": []          # derniers logs pour SSE
}

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def push_log(msg: str, level="info"):
    entry = {"time": datetime.now().strftime("%H:%M:%S"), "msg": msg, "level": level}
    bot_state["logs"].append(entry)
    if len(bot_state["logs"]) > 200:
        bot_state["logs"] = bot_state["logs"][-200:]


# ── Base de données ──────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id          TEXT PRIMARY KEY,
            titre       TEXT,
            prix        REAL,
            taille      TEXT,
            marque      TEXT,
            vendeur     TEXT,
            etat        TEXT,
            url         TEXT,
            image_url   TEXT,
            search_url  TEXT,
            vu_le       TEXT,
            alerte_sent INTEGER DEFAULT 0,
            favori      INTEGER DEFAULT 0
        )
    """)
    # Migration : ajouter la colonne etat si elle n'existe pas encore
    try:
        c.execute("ALTER TABLE articles ADD COLUMN etat TEXT DEFAULT ''")
    except Exception:
        pass
    c.execute("""
        CREATE TABLE IF NOT EXISTS watched_urls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            url         TEXT UNIQUE,
            label       TEXT,
            active      INTEGER DEFAULT 1,
            added_le    TEXT,
            last_scan   TEXT,
            articles_found INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Defaults
    for k, v in [
        ("discord_webhook", ""),
        ("interval_min", "120"),
        ("interval_max", "300"),
        ("prix_min", "0"),
        ("prix_max", "9999"),
        ("marge_min_alerte", "0"),
    ]:
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))
    conn.commit()
    conn.close()

def get_setting(key):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    r = c.fetchone()
    conn.close()
    return r[0] if r else ""

def set_setting(key, value):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    for k in ["search_id", "time"]:
        params.pop(k, None)
    params["page"] = ["1"]
    params["order"] = ["newest_first"]
    new_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=new_query))

def is_new(article_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM articles WHERE id=?", (article_id,))
    r = c.fetchone()
    conn.close()
    return r is None

def calc_score(article: dict) -> int:
    """Score d'opportunité 0-10 basé sur état + marge estimée + marque."""
    score = 0
    etat = (article.get("etat") or "").lower()
    if "étiquette" in etat and "avec" in etat:
        score += 3
    elif "sans étiquette" in etat:
        score += 2
    elif "très bon" in etat:
        score += 1
    # Marge estimée (revente x1.5)
    prix = article.get("prix", 0) or 0
    revente = prix * 1.5
    poids = 1200 if prix > 25 else (700 if prix > 15 else (400 if prix > 8 else 250))
    port = 2.99 if poids <= 500 else (3.99 if poids <= 2000 else (5.99 if poids <= 5000 else 6.99))
    fvinted = round(revente * 0.05 + 0.70, 2)
    marge = revente - prix - fvinted - port
    if marge >= 15:
        score += 4
    elif marge >= 8:
        score += 3
    elif marge >= 4:
        score += 2
    elif marge >= 0:
        score += 1
    # Marques premium connues
    marque = (article.get("marque") or "").lower()
    MARQUES_PREMIUM = {"nike","adidas","jordan","stone island","ralph lauren","tommy hilfiger",
                       "lacoste","levi's","levis","north face","supreme","off-white","balenciaga",
                       "gucci","louis vuitton","burberry","hugo boss","calvin klein","puma","new balance"}
    if any(m in marque for m in MARQUES_PREMIUM):
        score += 2
    return min(score, 10)

def save_article(article: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT OR IGNORE INTO articles
        (id,titre,prix,taille,marque,vendeur,etat,url,image_url,search_url,vu_le,alerte_sent)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,1)
    """, (
        article["id"], article["titre"], article["prix"],
        article.get("taille",""), article.get("marque",""),
        article.get("vendeur",""), article.get("etat",""),
        article["url"], article.get("image_url",""),
        article.get("search_url",""), datetime.now().isoformat()
    ))
    conn.commit()
    conn.close()

def get_articles(limit=100, search="", marque="", prix_max=None, prix_min=None, taille="", favoris_only=False, etat=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    q = "SELECT id,titre,prix,taille,marque,vendeur,etat,url,image_url,vu_le,favori FROM articles WHERE 1=1"
    params = []
    if search:
        q += " AND (titre LIKE ? OR marque LIKE ? OR vendeur LIKE ?)"
        params += [f"%{search}%"]*3
    if marque:
        q += " AND marque=?"
        params.append(marque)
    if prix_max is not None:
        q += " AND prix<=?"
        params.append(prix_max)
    if prix_min is not None:
        q += " AND prix>=?"
        params.append(prix_min)
    if taille:
        q += " AND taille=?"
        params.append(taille)
    if etat:
        q += " AND etat=?"
        params.append(etat)
    if favoris_only:
        q += " AND favori=1"
    q += " ORDER BY vu_le DESC LIMIT ?"
    params.append(limit)
    c.execute(q, params)
    cols = ["id","titre","prix","taille","marque","vendeur","etat","url","image_url","vu_le","favori"]
    rows = [dict(zip(cols, r)) for r in c.fetchall()]
    conn.close()
    # Enrichir avec le score d'opportunité
    for row in rows:
        row["score"] = calc_score(row)
    return rows

def get_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*), MIN(prix), MAX(prix), AVG(prix) FROM articles")
    total, mn, mx, avg = c.fetchone()
    c.execute("SELECT COUNT(*) FROM articles WHERE date(vu_le)=date('now')")
    today = c.fetchone()[0]
    c.execute("SELECT marque, COUNT(*) n FROM articles WHERE marque!='' GROUP BY marque ORDER BY n DESC LIMIT 6")
    marques = [{"marque": r[0], "count": r[1]} for r in c.fetchall()]
    c.execute("SELECT date(vu_le) d, COUNT(*) n FROM articles GROUP BY d ORDER BY d DESC LIMIT 7")
    par_jour = [{"date": r[0], "count": r[1]} for r in c.fetchall()]
    conn.close()
    return {
        "total": total or 0, "today": today or 0,
        "min_prix": round(mn or 0, 2), "max_prix": round(mx or 0, 2),
        "avg_prix": round(avg or 0, 2),
        "top_marques": marques, "par_jour": par_jour
    }

def get_watched_urls():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id,url,label,active,added_le,last_scan,articles_found FROM watched_urls ORDER BY id")
    cols = ["id","url","label","active","added_le","last_scan","articles_found"]
    rows = [dict(zip(cols, r)) for r in c.fetchall()]
    conn.close()
    return rows


# ── Discord ──────────────────────────────────────────────────────────────────
def send_discord_sync(article: dict, webhook_url: str):
    if not webhook_url:
        return
    prix = article['prix'] or 0
    revente = round(prix * 1.5, 2)
    poids = 1200 if prix > 25 else (700 if prix > 15 else (400 if prix > 8 else 250))
    port = 2.99 if poids <= 500 else (3.99 if poids <= 2000 else (5.99 if poids <= 5000 else 6.99))
    fvinted = round(revente * 0.05 + 0.70, 2)
    marge_nette = round(revente - prix - fvinted - port, 2)
    score = article.get("score", calc_score(article))
    score_label = "💎 Excellent" if score >= 8 else ("🔥 Bonne affaire" if score >= 5 else ("✅ Correct" if score >= 3 else ""))
    color = 0xFFD700 if score >= 8 else (0xFF4500 if score >= 5 else 0x09B1BA)
    marge_str = f"+{marge_nette} €" if marge_nette >= 0 else f"{marge_nette} €"
    etat = article.get("etat") or "—"
    title_prefix = "💎" if score >= 8 else ("🔥" if score >= 5 else "🆕")
    embed = {
        "title": f"{title_prefix} {article['titre'][:180]}",
        "url": article["url"],
        "color": color,
        "fields": [
            {"name": "💶 Prix achat", "value": f"{prix:.2f} €", "inline": True},
            {"name": "📈 Revente x1.5", "value": f"{revente} €", "inline": True},
            {"name": "💰 Marge nette", "value": marge_str, "inline": True},
            {"name": "📦 État", "value": etat, "inline": True},
            {"name": "📏 Taille", "value": article.get("taille") or "—", "inline": True},
            {"name": "🏷️ Marque", "value": article.get("marque") or "—", "inline": True},
            {"name": "👤 Vendeur", "value": article.get("vendeur") or "—", "inline": True},
        ],
        "footer": {"text": f"Vinted Bot • {datetime.now().strftime('%d/%m %H:%M')}{' • ' + score_label if score_label else ''}"},
    }
    if article.get("image_url"):
        embed["thumbnail"] = {"url": article["image_url"]}
    try:
        import requests as req
        req.post(webhook_url, json={"embeds": [embed]}, timeout=8)
    except Exception as e:
        log.error(f"Discord error: {e}")


# ── Scraper ──────────────────────────────────────────────────────────────────
def parse_prix(val) -> float:
    """Gère tous les formats de prix Vinted : float, str, dict {amount, currency}"""
    try:
        if isinstance(val, dict):
            return float(val.get("amount", val.get("amount_numeric", 0)) or 0)
        if isinstance(val, str):
            return float(val.replace(",", ".").replace(" ", "") or 0)
        return float(val or 0)
    except Exception:
        return 0.0


def run_bot():
    """Thread principal du bot — utilise requests + parsing HTML basique"""
    import requests
    from bs4 import BeautifulSoup

    push_log("Bot démarré", "success")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.vinted.fr/",
    })

    # Récupérer cookies en visitant d'abord la home
    try:
        session.get("https://www.vinted.fr/", timeout=15)
        time.sleep(2)
    except Exception:
        pass

    while bot_state["running"]:
        urls = get_watched_urls()
        active_urls = [u for u in urls if u["active"]]

        if not active_urls:
            push_log("Aucune URL active — ajoute une URL dans l'onglet Recherches", "warn")
            time.sleep(30)
            continue

        webhook = get_setting("discord_webhook")
        prix_max_filter = float(get_setting("prix_max") or 9999)
        prix_min_filter = float(get_setting("prix_min") or 0)
        marge_min_alerte = float(get_setting("marge_min_alerte") or 0)

        for wu in active_urls:
            if not bot_state["running"]:
                break

            clean_url = normalize_url(wu["url"])
            push_log(f"Scan: {wu['label'] or clean_url[:50]}...")

            try:
                # Appel API JSON Vinted (plus fiable que HTML)
                api_url = clean_url.replace("vinted.fr/catalog", "vinted.fr/api/v2/catalog/items")
                r = session.get(api_url, timeout=20, params={"per_page": 48})

                articles_found = []

                if r.status_code == 200 and "items" in r.text:
                    data = r.json()
                    items = data.get("items", [])
                    for item in items[:40]:
                        photo_url = ""
                        if isinstance(item.get("photo"), dict):
                            photo_url = item["photo"].get("url", "")
                        art_id = str(item.get("id", ""))
                        if not art_id:
                            continue
                        slug = re.sub(r'[^a-z0-9]+', '-', item.get('title','').lower()).strip('-')[:40]
                        art_url = item.get('url', '') or f'https://www.vinted.fr/items/{art_id}-{slug}'
                        if art_url.startswith('/'):
                            art_url = 'https://www.vinted.fr' + art_url
                        articles_found.append({
                            "id": art_id,
                            "titre": item.get("title", "Article"),
                            "prix": parse_prix(item.get("price", item.get("price_numeric", 0))),
                            "taille": item.get("size_title", ""),
                            "marque": item.get("brand_title", ""),
                            "vendeur": item.get("user", {}).get("login", "") if isinstance(item.get("user"), dict) else "",
                            "etat": item.get("status_title", item.get("condition_title", item.get("status", ""))),
                            "url": art_url,
                            "image_url": photo_url,
                            "search_url": wu["url"]
                        })
                else:
                    # Fallback HTML
                    r2 = session.get(clean_url, timeout=20)
                    soup = BeautifulSoup(r2.text, "html.parser")
                    # Chercher __NEXT_DATA__
                    script = soup.find("script", {"id": "__NEXT_DATA__"})
                    if script:
                        nd = json.loads(script.string)
                        items = (nd.get("props", {}).get("pageProps", {})
                                   .get("catalogItems", {}).get("catalogItems", []))
                        for item in items[:40]:
                            art_id = str(item.get("id", ""))
                            if not art_id:
                                continue
                            photo_url = item.get("photo", {}).get("url", "") if isinstance(item.get("photo"), dict) else ""
                            articles_found.append({
                                "id": art_id,
                                "titre": item.get("title", "Article"),
                                "prix": parse_prix(item.get("price", item.get("price_numeric", 0))),
                                "taille": item.get("size_title", ""),
                                "marque": item.get("brand_title", ""),
                                "vendeur": item.get("user", {}).get("login", "") if isinstance(item.get("user"), dict) else "",
                                "etat": item.get("status_title", item.get("condition_title", item.get("status", ""))),
                                "url": f"https://www.vinted.fr/items/{art_id}",
                                "image_url": photo_url,
                                "search_url": wu["url"]
                            })

                new_count = 0
                for art in articles_found:
                    if art["prix"] > prix_max_filter:
                        continue
                    if art["prix"] < prix_min_filter:
                        continue
                    if is_new(art["id"]):
                        score = calc_score(art)
                        art["score"] = score
                        save_article(art)
                        new_count += 1
                        bot_state["new_today"] += 1
                        score_label = " 💎" if score >= 8 else (" 🔥" if score >= 5 else "")
                        push_log(f"  ✅ Nouveau: {art['titre'][:35]} — {art['prix']}€{score_label}", "success")
                        if webhook:
                            # Vérifier marge minimum avant d'alerter
                            prix = art["prix"]
                            revente = prix * 1.5
                            poids = 1200 if prix > 25 else (700 if prix > 15 else (400 if prix > 8 else 250))
                            port = 2.99 if poids <= 500 else (3.99 if poids <= 2000 else (5.99 if poids <= 5000 else 6.99))
                            fvinted = round(revente * 0.05 + 0.70, 2)
                            marge_nette = revente - prix - fvinted - port
                            if marge_nette >= marge_min_alerte:
                                threading.Thread(target=send_discord_sync, args=(art, webhook), daemon=True).start()
                        time.sleep(random.uniform(0.3, 0.8))

                # Update last_scan
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("UPDATE watched_urls SET last_scan=?, articles_found=articles_found+? WHERE id=?",
                          (datetime.now().isoformat(), new_count, wu["id"]))
                conn.commit()
                conn.close()

                if new_count == 0:
                    push_log(f"  → Rien de nouveau ({len(articles_found)} articles scannés)")
                else:
                    push_log(f"  → {new_count} nouveaux articles !", "success")

            except Exception as e:
                push_log(f"Erreur scan: {e}", "error")
                log.error(f"Scan error: {e}")

            # Délai entre URLs
            if len(active_urls) > 1:
                time.sleep(random.uniform(4, 8))

        bot_state["last_scan"] = datetime.now().strftime("%H:%M:%S")
        bot_state["scan_count"] += 1

        if bot_state.get("scan_once"):
            bot_state["scan_once"] = False
            bot_state["running"] = False
            push_log("Scan unique terminé", "success")
            break

        interval_min = int(get_setting("interval_min") or 120)
        interval_max = int(get_setting("interval_max") or 300)
        wait = random.uniform(interval_min, interval_max)
        push_log(f"Prochain scan dans {int(wait)}s")

        # Attente interruptible
        for _ in range(int(wait)):
            if not bot_state["running"]:
                break
            time.sleep(1)

    push_log("Bot arrêté", "warn")


# ── Routes API ───────────────────────────────────────────────────────────────
@app.route("/api/articles")
def api_articles():
    search = request.args.get("search", "")
    marque = request.args.get("marque", "")
    taille = request.args.get("taille", "")
    etat   = request.args.get("etat", "")
    prix_max = request.args.get("prix_max")
    prix_min = request.args.get("prix_min")
    favoris = request.args.get("favoris") == "1"
    limit = int(request.args.get("limit", 9999))
    return jsonify(get_articles(
        limit, search, marque,
        float(prix_max) if prix_max else None,
        float(prix_min) if prix_min else None,
        taille, favoris, etat
    ))

@app.route("/api/articles/export")
def api_articles_export():
    import csv, io
    favoris = request.args.get("favoris") == "1"
    articles = get_articles(limit=99999, favoris_only=favoris)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["id","titre","prix","taille","marque","vendeur","etat","score","url","image_url","vu_le","favori"])
    writer.writeheader()
    writer.writerows(articles)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=vinted_articles.csv"}
    )

@app.route("/api/etats")
def api_etats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT etat FROM articles WHERE etat!='' ORDER BY etat")
    etats = [r[0] for r in c.fetchall()]
    conn.close()
    # Ordre logique
    ordre = ["Neuf avec étiquette","Neuf sans étiquette","Très bon état","Bon état","Satisfaisant"]
    return jsonify(sorted(etats, key=lambda e: ordre.index(e) if e in ordre else 99))

@app.route("/api/tailles")
def api_tailles():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT taille FROM articles WHERE taille!='' ORDER BY taille")
    tailles = [r[0] for r in c.fetchall()]
    conn.close()
    return jsonify(tailles)

@app.route("/api/discord/test", methods=["POST"])
def api_discord_test():
    webhook = get_setting("discord_webhook")
    if not webhook:
        return jsonify({"ok": False, "msg": "Aucun webhook configuré"})
    try:
        import requests as req
        r = req.post(webhook, json={
            "embeds": [{
                "title": "✅ Test Vinted Bot",
                "description": "La connexion Discord fonctionne correctement !",
                "color": 0x09B1BA,
                "footer": {"text": f"Vinted Bot • {datetime.now().strftime('%d/%m %H:%M')}"}
            }]
        }, timeout=8)
        if r.status_code in (200, 204):
            return jsonify({"ok": True})
        return jsonify({"ok": False, "msg": f"Discord a répondu {r.status_code}"})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())

@app.route("/api/bot/status")
def api_bot_status():
    return jsonify({
        "running": bot_state["running"],
        "last_scan": bot_state["last_scan"],
        "scan_count": bot_state["scan_count"],
        "new_today": bot_state["new_today"],
        "logs": bot_state["logs"][-50:]
    })

@app.route("/api/bot/start", methods=["POST"])
def api_bot_start():
    if bot_state["running"]:
        return jsonify({"ok": False, "msg": "Déjà en cours"})
    bot_state["running"] = True
    bot_state["new_today"] = 0
    t = threading.Thread(target=run_bot, daemon=True)
    bot_state["thread"] = t
    t.start()
    return jsonify({"ok": True})

@app.route("/api/bot/stop", methods=["POST"])
def api_bot_stop():
    bot_state["running"] = False
    return jsonify({"ok": True})

@app.route("/api/bot/scan-now", methods=["POST"])
def api_bot_scan_now():
    if bot_state["running"]:
        return jsonify({"ok": False, "msg": "Bot déjà en cours, il scannera à son prochain cycle"})
    bot_state["running"] = True
    bot_state["scan_once"] = True
    def run_once():
        run_bot()
    t = threading.Thread(target=run_once, daemon=True)
    t.start()
    return jsonify({"ok": True})

@app.route("/api/urls", methods=["GET"])
def api_urls_get():
    return jsonify(get_watched_urls())

@app.route("/api/urls", methods=["POST"])
def api_urls_add():
    data = request.json
    url = data.get("url", "").strip()
    label = data.get("label", "").strip()
    if not url or "vinted.fr" not in url:
        return jsonify({"ok": False, "msg": "URL invalide"})
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO watched_urls (url,label,active,added_le) VALUES (?,?,1,?)",
                  (url, label or url[:50], datetime.now().isoformat()))
        conn.commit()
        push_log(f"URL ajoutée: {label or url[:40]}", "success")
        return jsonify({"ok": True})
    except sqlite3.IntegrityError:
        return jsonify({"ok": False, "msg": "URL déjà ajoutée"})
    finally:
        conn.close()

@app.route("/api/urls/<int:uid>", methods=["DELETE"])
def api_urls_delete(uid):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM watched_urls WHERE id=?", (uid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/urls/<int:uid>/toggle", methods=["POST"])
def api_urls_toggle(uid):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE watched_urls SET active=1-active WHERE id=?", (uid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    keys = ["discord_webhook","interval_min","interval_max","prix_min","prix_max","marge_min_alerte"]
    return jsonify({k: get_setting(k) for k in keys})

@app.route("/api/settings", methods=["POST"])
def api_settings_save():
    data = request.json
    for k, v in data.items():
        set_setting(k, v)
    push_log("Paramètres sauvegardés", "success")
    return jsonify({"ok": True})

@app.route("/api/articles/<aid>/favori", methods=["POST"])
def api_favori(aid):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE articles SET favori=1-favori WHERE id=?", (aid,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/marques")
def api_marques():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT marque FROM articles WHERE marque!='' ORDER BY marque")
    marques = [r[0] for r in c.fetchall()]
    conn.close()
    return jsonify(marques)

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    init_db()
    log.info("Vinted Bot démarré → http://localhost:5000")
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
