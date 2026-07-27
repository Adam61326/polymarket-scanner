#!/usr/bin/env python3
"""
Polymarket Opportunity Scanner
===============================

Deux modules de détection, activables indépendamment :

1. MISPRICING  — tu donnes ta propre estimation de probabilité sur des
   marchés que tu suis (watchlist), le script la compare au prix affiché
   par Polymarket et t'alerte si l'écart ("edge") dépasse un seuil.

2. WHALE TRACKING — le script surveille les wallets les mieux classés
   du leaderboard (ou une liste de wallets que tu fournis) et t'alerte
   quand ils ouvrent une position importante récente, pour repérer où va
   le "smart money" avant que le prix ne bouge.

Aucune clé API n'est nécessaire : tous les endpoints utilisés sont en
lecture publique (Gamma API + Data API). Rien n'est jamais tradé
automatiquement — ce script ne fait qu'observer et alerter.

Installation :
    pip install requests

Utilisation :
    python polymarket_opportunities.py --once          # un seul passage
    python polymarket_opportunities.py --loop 300       # boucle toutes les 300s
    python polymarket_opportunities.py --loop 300 --webhook <URL_DISCORD_OU_SLACK>

Configuration : édite WATCHLIST et WHALE_WALLETS ci-dessous, ou passe-les
dans watchlist.json / whale_wallets.json (voir generate_config_templates()).
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

# Marchés que tu suis, avec TA probabilité estimée (0 à 1).
# "slug" = le dernier segment de l'URL polymarket.com/event/<slug>
# Exemple : pour https://polymarket.com/event/will-x-happen -> slug = "will-x-happen"
WATCHLIST = [
    # {"slug": "exemple-de-marche", "my_probability": 0.55, "outcome": "Yes"},
]

# Seuil d'écart (en points de probabilité) au-delà duquel on alerte.
# 0.10 = alerte si ton estimation diverge du prix de marché de 10 points ou plus.
MIN_EDGE = 0.10

# Wallets à surveiller pour le whale tracking. Laisse vide pour utiliser
# automatiquement le top N du leaderboard.
WHALE_WALLETS = []  # ex: ["0xabc123...", "0xdef456..."]
LEADERBOARD_TOP_N = 50

# Pilier 1 — copy trading affiné : ne suivre que les traders dont la
# performance sur les marchés Politique est confirmée récemment (top
# "tout temps" ET top "dernier mois"), pas juste les plus gros PNL toutes
# catégories confondues (souvent un seul pari géant gagné une fois).
WHALE_QUALIFIED_ONLY = True

# Taille minimale (en USD) d'une transaction pour déclencher une alerte whale.
WHALE_MIN_TRADE_USD = 5000

# Fichier local servant de mémoire pour ne pas ré-alerter deux fois sur le
# même trade.
SEEN_TRADES_FILE = os.path.join(os.path.dirname(__file__), "seen_trades.json")

# --- Nouveaux marchés (filtré Politique / Géopolitique) ---
# Alerte dès qu'un nouveau marché politique/géopolitique est créé sur
# Polymarket. Pas d'estimation automatique : juste la question + le prix
# actuel, à toi de juger.
NEW_MARKETS_ENABLED = True
NEW_MARKETS_TAG_IDS = [2, 100265]  # 2 = Politics, 100265 = Geopolitics
NEW_MARKETS_SCAN_LIMIT = 100  # nb de marchés les plus récents examinés par tag, par passage
NEW_MARKETS_MIN_LIQUIDITY = 0  # filtre optionnel, en USD (0 = pas de filtre)
SEEN_MARKETS_FILE = os.path.join(os.path.dirname(__file__), "seen_markets.json")

# --- Pilier 3 : vérification structurelle (marge par événement) ---
# Sur un événement à résultats multiples et mutuellement exclusifs (ex:
# "Qui sera le prochain PM ?" avec 5 candidats), la somme des prix "Yes"
# devrait être proche de 100%. L'écart au-dessus (marge) rémunère les
# teneurs de marché ; une marge anormalement élevée signale un marché peu
# arbitré (candidat à surveiller). Une marge NÉGATIVE (rarissime) est un
# arbitrage mathématique réel, peu importe qui gagne.
MARGIN_ENABLED = True
MARGIN_SCAN_LIMIT = 100  # nb d'événements les plus actifs examinés par tag, par passage
MARGIN_HIGH_THRESHOLD = 0.15  # 15% — au-delà, on considère le marché peu efficient
SEEN_MARGIN_FILE = os.path.join(os.path.dirname(__file__), "seen_margin_alerts.json")

# --- Pilier 2 : workflow d'actualité ---
# Le vrai edge sur des marchés de niche vient souvent du DÉLAI entre une
# actualité et sa prise en compte par le prix, pas d'un avis "meilleur".
# Ces flux RSS gratuits (aucune clé requise) sont scannés à chaque passage ;
# toute nouvelle actu détectée est alertée pour que tu vérifies si le marché
# correspondant a déjà bougé ou non.
NEWS_ENABLED = True
NEWS_FEEDS = {
    "Général": [
        "https://news.google.com/rss/search?q=world%20news%20when:1h&hl=en-US&gl=US&ceid=US:en",
    ],
    "Moyen-Orient/Iran": [
        "http://feeds.bbci.co.uk/news/world/middle_east/rss.xml",
        "https://news.google.com/rss/search?q=Iran%20when:2h&hl=en-US&gl=US&ceid=US:en",
    ],
    "Elections US": [
        "https://news.google.com/rss/search?q=Trump%20OR%20Congress%20when:2h&hl=en-US&gl=US&ceid=US:en",
    ],
    "Europe": [
        "http://feeds.bbci.co.uk/news/world/europe/rss.xml",
        "https://news.google.com/rss/search?q=Europe%20election%20when:2h&hl=en-US&gl=US&ceid=US:en",
    ],
}
SEEN_NEWS_FILE = os.path.join(os.path.dirname(__file__), "seen_news.json")

# --- Notifications téléphone (ntfy.sh) ---
# Laisse vide pour désactiver. Sinon, choisis un nom de topic unique et
# difficile à deviner (ex: "polymarket-adam-x7k2"), installe l'app ntfy sur
# ton téléphone, abonne-toi à ce topic — c'est tout, aucun compte requis.
NTFY_TOPIC = ""  # ex: "polymarket-adam-x7k2"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("polymarket-scanner")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "polymarket-opportunity-scanner/1.0"})


# ---------------------------------------------------------------------------
# Helpers réseau
# ---------------------------------------------------------------------------

def _get(url, params=None, retries=3, timeout=10):
    for attempt in range(1, retries + 1):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            log.warning("Échec requête (%s/%s) sur %s : %s", attempt, retries, url, exc)
            time.sleep(1.5 * attempt)
    log.error("Abandon après %s tentatives : %s", retries, url)
    return None


def _get_raw(url, retries=3, timeout=10):
    """Comme _get, mais renvoie le texte brut (pour du RSS/XML, pas du JSON)."""
    for attempt in range(1, retries + 1):
        try:
            resp = SESSION.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as exc:
            log.warning("Échec requête RSS (%s/%s) sur %s : %s", attempt, retries, url, exc)
            time.sleep(1.5 * attempt)
    log.error("Abandon après %s tentatives (RSS) : %s", retries, url)
    return None


# ---------------------------------------------------------------------------
# Module 1 — Mispricing (ton estimation vs le marché)
# ---------------------------------------------------------------------------

def get_market_by_slug(slug):
    """Récupère un marché Gamma API via son slug d'event."""
    data = _get(f"{GAMMA_API}/events", params={"slug": slug})
    if not data:
        return None
    if isinstance(data, list) and data:
        event = data[0]
        markets = event.get("markets", [])
        return markets[0] if markets else None
    return None


def current_price_for_outcome(market, outcome_label):
    """Extrait le prix courant (probabilité implicite) pour Yes/No."""
    try:
        outcomes = json.loads(market.get("outcomes", "[]"))
        prices = json.loads(market.get("outcomePrices", "[]"))
    except (json.JSONDecodeError, TypeError):
        return None
    for label, price in zip(outcomes, prices):
        if label.lower() == outcome_label.lower():
            return float(price)
    return None


def scan_mispricing():
    log.info("=== Scan mispricing (%d marché(s) suivis) ===", len(WATCHLIST))
    alerts = []
    for item in WATCHLIST:
        slug = item["slug"]
        my_prob = item["my_probability"]
        outcome = item.get("outcome", "Yes")

        market = get_market_by_slug(slug)
        if not market:
            log.warning("Marché introuvable pour slug=%s", slug)
            continue

        market_price = current_price_for_outcome(market, outcome)
        if market_price is None:
            log.warning("Prix introuvable pour %s / %s", slug, outcome)
            continue

        edge = my_prob - market_price
        question = market.get("question", slug)

        if abs(edge) >= MIN_EDGE:
            direction = "SOUS-évalué (achète)" if edge > 0 else "SUR-évalué (vends/évite)"
            msg = (
                f"[MISPRICING] {question}\n"
                f"   Prix marché ({outcome}) : {market_price:.2%}  |  "
                f"Ton estimation : {my_prob:.2%}  |  Edge : {edge:+.1%}\n"
                f"   -> {direction}"
            )
            log.info(msg)
            alerts.append(msg)
        else:
            log.info(
                "OK, pas d'edge significatif — %s : marché %.1f%% vs toi %.1f%%",
                question, market_price * 100, my_prob * 100,
            )
    return alerts


# ---------------------------------------------------------------------------
# Module 2 — Whale tracking (smart money)
# ---------------------------------------------------------------------------

def get_leaderboard(limit=LEADERBOARD_TOP_N, category="OVERALL", time_period="ALL"):
    data = _get(
        f"{DATA_API}/v1/leaderboard",
        params={
            "limit": min(limit, 50),
            "timePeriod": time_period,
            "orderBy": "PNL",
            "category": category,
        },
    )
    if not data:
        return []
    return [row.get("proxyWallet") for row in data if row.get("proxyWallet")]


def get_qualified_traders(limit=LEADERBOARD_TOP_N):
    """Pilier 1 — copy trading affiné.

    Au lieu de suivre les plus gros PNL toutes catégories confondues (qui
    peuvent être un seul pari géant gagné une fois), on ne retient que les
    traders qui apparaissent À LA FOIS dans le top Politique 'tout temps'
    ET dans le top Politique 'dernier mois' — un signal de performance
    répétée et récente sur ce créneau précis, pas un coup de chance ancien.
    """
    if not WHALE_QUALIFIED_ONLY:
        return get_leaderboard(limit=limit, category="OVERALL", time_period="ALL")

    all_time = set(get_leaderboard(limit=50, category="POLITICS", time_period="ALL"))
    last_month = set(get_leaderboard(limit=50, category="POLITICS", time_period="MONTH"))
    qualified = list(all_time & last_month)

    log.info(
        "Traders qualifiés (top Politique tout-temps ET dernier mois) : %d",
        len(qualified),
    )

    if len(qualified) < 5:
        log.warning(
            "Peu de traders qualifiés (%d) — complément avec le top Politique "
            "'tout temps' pour avoir une base suffisante.",
            len(qualified),
        )
        qualified = list(set(qualified) | all_time)

    return qualified[:limit]


def get_recent_activity(wallet, limit=20):
    """Renvoie les dernières transactions d'un wallet (achats/ventes)."""
    data = _get(f"{DATA_API}/activity", params={"user": wallet, "limit": limit})
    return data or []


def load_seen_trades():
    if os.path.exists(SEEN_TRADES_FILE):
        try:
            with open(SEEN_TRADES_FILE, "r") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, OSError):
            return set()
    return set()


def save_seen_trades(seen):
    trimmed = list(seen)[-5000:]
    with open(SEEN_TRADES_FILE, "w") as f:
        json.dump(trimmed, f)


def scan_whales():
    wallets = WHALE_WALLETS or get_qualified_traders()
    log.info("=== Scan whale tracking (%d wallet(s)) ===", len(wallets))

    seen = load_seen_trades()
    alerts = []

    for wallet in wallets:
        activity = get_recent_activity(wallet)
        for trade in activity:
            trade_id = trade.get("transactionHash") or trade.get("id")
            if not trade_id or trade_id in seen:
                continue
            seen.add(trade_id)

            usd_size = float(trade.get("usdcSize") or trade.get("size", 0) or 0)
            if usd_size < WHALE_MIN_TRADE_USD:
                continue

            title = trade.get("title") or trade.get("market", "Marché inconnu")
            outcome = trade.get("outcome", "?")
            side = trade.get("side", "?")

            msg = (
                f"[WHALE] {wallet[:10]}... vient de {side} {usd_size:,.0f}$ "
                f"sur '{title}' ({outcome})"
            )
            log.info(msg)
            alerts.append(msg)

    save_seen_trades(seen)
    return alerts


# ---------------------------------------------------------------------------
# Module 3 — Nouveaux marchés (tous secteurs, alerte simple)
# ---------------------------------------------------------------------------

def load_seen_markets():
    if os.path.exists(SEEN_MARKETS_FILE):
        try:
            with open(SEEN_MARKETS_FILE, "r") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, OSError):
            return set()
    return set()


def save_seen_markets(seen):
    trimmed = list(seen)[-10000:]
    with open(SEEN_MARKETS_FILE, "w") as f:
        json.dump(trimmed, f)


def get_recent_markets(limit=NEW_MARKETS_SCAN_LIMIT):
    """Récupère les marchés actifs les plus récents, filtrés par tag
    (Politique / Géopolitique)."""
    seen_ids = set()
    markets = []

    for tag_id in NEW_MARKETS_TAG_IDS:
        data = _get(
            f"{GAMMA_API}/events",
            params={
                "active": "true",
                "closed": "false",
                "order": "id",
                "ascending": "false",
                "limit": limit,
                "tag_id": tag_id,
            },
        )
        if not data:
            continue

        for event in data:
            for market in event.get("markets", []):
                market_id = market.get("id")
                if market_id in seen_ids:
                    continue
                seen_ids.add(market_id)
                market["_event_slug"] = event.get("slug", "")
                market["_event_title"] = event.get("title", event.get("ticker", ""))
                markets.append(market)

    return markets


def scan_new_markets():
    if not NEW_MARKETS_ENABLED:
        return []

    log.info("=== Scan nouveaux marchés (Politique / Géopolitique) ===")
    seen = load_seen_markets()
    is_first_run = len(seen) == 0
    markets = get_recent_markets()
    alerts = []

    for market in markets:
        market_id = str(market.get("id", ""))
        if not market_id or market_id in seen:
            continue
        seen.add(market_id)

        if is_first_run:
            continue

        liquidity = float(market.get("liquidityNum") or market.get("liquidity", 0) or 0)
        if liquidity < NEW_MARKETS_MIN_LIQUIDITY:
            continue

        question = market.get("question") or market.get("_event_title") or market_id
        yes_price = current_price_for_outcome(market, "Yes")
        price_str = f"{yes_price:.0%}" if yes_price is not None else "n/a"
        slug = market.get("_event_slug", "")
        url = f"https://polymarket.com/event/{slug}" if slug else ""

        msg = f"[NOUVEAU MARCHÉ] {question}\n   Prix Yes actuel : {price_str}"
        if url:
            msg += f"\n   {url}"

        log.info(msg)
        alerts.append(msg)

    save_seen_markets(seen)
    if is_first_run:
        log.info(
            "Premier passage : %d marché(s) existants enregistrés comme référence, "
            "pas d'alerte. Les prochains passages n'alerteront que sur les vraies nouveautés.",
            len(seen),
        )
    return alerts


# ---------------------------------------------------------------------------
# Module 4 — Pilier 2 : workflow d'actualité (RSS, réduire le délai d'info)
# ---------------------------------------------------------------------------

def load_seen_news():
    if os.path.exists(SEEN_NEWS_FILE):
        try:
            with open(SEEN_NEWS_FILE, "r") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, OSError):
            return set()
    return set()


def save_seen_news(seen):
    trimmed = list(seen)[-5000:]
    with open(SEEN_NEWS_FILE, "w") as f:
        json.dump(trimmed, f)


def parse_rss_items(xml_text):
    """Parse un flux RSS 2.0 basique et renvoie une liste de (titre, lien)."""
    items = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items

    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if title:
            items.append((title, link))
    return items


def scan_news():
    if not NEWS_ENABLED:
        return []

    log.info("=== Scan actualité (Général + Moyen-Orient/Iran + Elections US + Europe) ===")
    seen = load_seen_news()
    is_first_run = len(seen) == 0
    alerts = []

    for category, urls in NEWS_FEEDS.items():
        for url in urls:
            xml_text = _get_raw(url)
            if not xml_text:
                continue

            for title, link in parse_rss_items(xml_text):
                key = hashlib.md5(f"{title}|{link}".encode("utf-8")).hexdigest()
                if key in seen:
                    continue
                seen.add(key)

                if is_first_run:
                    continue

                msg = f"[ACTUALITE - {category}] {title}"
                if link:
                    msg += f"\n   {link}"

                log.info(msg)
                alerts.append(msg)

    save_seen_news(seen)
    if is_first_run:
        log.info(
            "Premier passage actualité : %d article(s) enregistrés comme référence, "
            "pas d'alerte. Les prochains passages n'alerteront que sur les vraies nouveautés.",
            len(seen),
        )
    return alerts


# ---------------------------------------------------------------------------
# Module 5 — Pilier 3 : vérification structurelle (marge par événement)
# ---------------------------------------------------------------------------

def load_seen_margin_alerts():
    if os.path.exists(SEEN_MARGIN_FILE):
        try:
            with open(SEEN_MARGIN_FILE, "r") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, OSError):
            return set()
    return set()


def save_seen_margin_alerts(seen):
    trimmed = list(seen)[-5000:]
    with open(SEEN_MARGIN_FILE, "w") as f:
        json.dump(trimmed, f)


def compute_event_margin(event):
    """Somme des prix 'Yes' de tous les résultats d'un événement multi-choix.
    Renvoie None si l'événement n'a pas au moins 3 résultats (pas assez
    'multi-choix' pour que la marge veuille dire grand-chose) ou si un prix
    manque. Marge = somme - 100% (ex: 0.05 = 5% au-dessus de 100%)."""
    markets = event.get("markets", [])
    if len(markets) < 3:
        return None

    total = 0.0
    for market in markets:
        price = current_price_for_outcome(market, "Yes")
        if price is None:
            return None
        total += price

    return total - 1.0


def scan_margin_anomalies():
    if not MARGIN_ENABLED:
        return []

    log.info("=== Scan marge structurelle (événements multi-résultats) ===")
    seen = load_seen_margin_alerts()
    alerts = []

    for tag_id in NEW_MARKETS_TAG_IDS:
        data = _get(
            f"{GAMMA_API}/events",
            params={
                "active": "true",
                "closed": "false",
                "order": "volume",
                "ascending": "false",
                "limit": MARGIN_SCAN_LIMIT,
                "tag_id": tag_id,
            },
        )
        if not data:
            continue

        for event in data:
            margin = compute_event_margin(event)
            if margin is None:
                continue

            is_anomaly = margin < 0 or margin >= MARGIN_HIGH_THRESHOLD
            if not is_anomaly:
                continue

            event_id = str(event.get("id", ""))
            if not event_id or event_id in seen:
                continue
            seen.add(event_id)

            title = event.get("title") or event.get("ticker") or event_id
            slug = event.get("slug", "")
            url = f"https://polymarket.com/event/{slug}" if slug else ""
            n_outcomes = len(event.get("markets", []))

            if margin < 0:
                kind = "ARBITRAGE POSSIBLE (marge negative)"
            else:
                kind = "Marge anormalement elevee (marche peu arbitre)"

            msg = (
                f"[MARGE] {kind}\n"
                f"   {title}\n"
                f"   Marge : {margin:+.1%} sur {n_outcomes} resultats"
            )
            if url:
                msg += f"\n   {url}"

            log.info(msg)
            alerts.append(msg)

    save_seen_margin_alerts(seen)
    return alerts


# ---------------------------------------------------------------------------
# Mode screening — export CSV de TOUS les marchés Politique/Géopolitique
# ---------------------------------------------------------------------------

def fetch_all_markets_for_tag(tag_id, page_size=100, max_pages=50):
    """Récupère tous les marchés actifs d'un tag, en paginant avec offset."""
    all_markets = []
    offset = 0

    for _ in range(max_pages):
        data = _get(
            f"{GAMMA_API}/events",
            params={
                "active": "true",
                "closed": "false",
                "order": "liquidity",
                "ascending": "true",
                "limit": page_size,
                "offset": offset,
                "tag_id": tag_id,
            },
        )
        if not data:
            break

        for event in data:
            for market in event.get("markets", []):
                market["_event_slug"] = event.get("slug", "")
                market["_event_title"] = event.get("title", event.get("ticker", ""))
                market["_tag_id"] = tag_id
                all_markets.append(market)

        if len(data) < page_size:
            break
        offset += page_size

    return all_markets


def run_screening(output_path=None):
    """Récupère tous les marchés Politique + Géopolitique actifs et exporte
    un CSV trié du moins liquide au plus liquide."""
    import csv

    output_path = output_path or os.path.join(
        os.path.dirname(__file__),
        f"screening_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
    )

    log.info("=== Screening complet Politique + Géopolitique (peut prendre 1-2 min) ===")
    seen_ids = set()
    rows = []

    for tag_id in NEW_MARKETS_TAG_IDS:
        label = "Politics" if tag_id == 2 else "Geopolitics"
        log.info("Récupération de tous les marchés actifs — tag %s (%s)...", tag_id, label)
        markets = fetch_all_markets_for_tag(tag_id)
        log.info("  -> %d marché(s) récupérés pour %s", len(markets), label)

        for market in markets:
            market_id = market.get("id")
            if not market_id or market_id in seen_ids:
                continue
            seen_ids.add(market_id)

            question = market.get("question") or market.get("_event_title") or market_id
            yes_price = current_price_for_outcome(market, "Yes")
            liquidity = float(market.get("liquidityNum") or market.get("liquidity", 0) or 0)
            volume = float(market.get("volumeNum") or market.get("volume", 0) or 0)
            slug = market.get("_event_slug", "")
            url = f"https://polymarket.com/event/{slug}" if slug else ""
            end_date = market.get("endDate", "")

            rows.append({
                "categorie": label,
                "question": question,
                "prix_yes": f"{yes_price:.2%}" if yes_price is not None else "",
                "liquidite_usd": round(liquidity, 2),
                "volume_usd": round(volume, 2),
                "date_fin": end_date,
                "url": url,
            })

    rows.sort(key=lambda r: r["liquidite_usd"])

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "categorie", "question", "prix_yes", "liquidite_usd",
            "volume_usd", "date_fin", "url",
        ])
        writer.writeheader()
        writer.writerows(rows)

    log.info(
        "Screening terminé : %d marché(s) au total exportés dans %s",
        len(rows), output_path,
    )
    log.info(
        "Les lignes en haut du fichier (liquidité la plus faible) sont celles "
        "où un edge personnel a statistiquement le plus de chances de compter."
    )
    return output_path


# ---------------------------------------------------------------------------
# Notifications (optionnel — webhook Discord/Slack)
# ---------------------------------------------------------------------------

def send_webhook(webhook_url, alerts):
    if not webhook_url or not alerts:
        return
    content = "\n\n".join(alerts)
    try:
        SESSION.post(webhook_url, json={"content": content[:1900]}, timeout=10)
    except requests.exceptions.RequestException as exc:
        log.warning("Échec envoi webhook : %s", exc)


def send_ntfy_notification(topic, alerts):
    """Envoie une notification push sur le téléphone via ntfy.sh."""
    if not topic or not alerts:
        return
    body = "\n\n".join(alerts)[:3900]
    try:
        SESSION.post(
            f"https://ntfy.sh/{topic}",
            data=body.encode("utf-8"),
            headers={
                "Title": f"Polymarket - {len(alerts)} alerte(s)",
                "Priority": "default",
                "Tags": "moneybag",
            },
            timeout=10,
        )
    except requests.exceptions.RequestException as exc:
        log.warning("Échec envoi notification ntfy.sh : %s", exc)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def generate_config_templates():
    """Crée des fichiers JSON exemples pour éditer la config sans toucher au code."""
    watchlist_path = os.path.join(os.path.dirname(__file__), "watchlist.example.json")
    wallets_path = os.path.join(os.path.dirname(__file__), "whale_wallets.example.json")

    with open(watchlist_path, "w") as f:
        json.dump(
            [{"slug": "exemple-de-marche", "my_probability": 0.55, "outcome": "Yes"}],
            f, indent=2,
        )
    with open(wallets_path, "w") as f:
        json.dump(["0xWALLET_A", "0xWALLET_B"], f, indent=2)

    log.info("Templates créés : %s, %s", watchlist_path, wallets_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_once(webhook_url=None, ntfy_topic=None):
    log.info("Passage démarré à %s", datetime.now(timezone.utc).isoformat())
    all_alerts = []
    all_alerts += scan_mispricing()
    all_alerts += scan_whales()
    all_alerts += scan_new_markets()
    all_alerts += scan_news()
    all_alerts += scan_margin_anomalies()
    if webhook_url:
        send_webhook(webhook_url, all_alerts)
    if ntfy_topic:
        send_ntfy_notification(ntfy_topic, all_alerts)
    log.info("Passage terminé — %d alerte(s)", len(all_alerts))
    return all_alerts


def main():
    parser = argparse.ArgumentParser(description="Polymarket Opportunity Scanner")
    parser.add_argument("--once", action="store_true", help="Un seul passage puis quitte")
    parser.add_argument("--loop", type=int, metavar="SECONDS",
                         help="Boucle en continu toutes les N secondes")
    parser.add_argument("--webhook", type=str, default=None,
                         help="URL webhook Discord/Slack pour recevoir les alertes")
    parser.add_argument("--ntfy-topic", type=str, default=None,
                         help="Nom du topic ntfy.sh pour recevoir les alertes sur ton téléphone "
                              "(remplace la valeur de NTFY_TOPIC dans le script si fourni)")
    parser.add_argument("--init-config", action="store_true",
                         help="Génère des fichiers de config exemples et quitte")
    parser.add_argument("--screen", action="store_true",
                         help="Exporte un CSV de tous les marchés Politique/Géopolitique "
                              "actifs, triés du moins liquide au plus liquide")
    args = parser.parse_args()

    if args.screen:
        run_screening()
        return

    if args.init_config:
        generate_config_templates()
        return

    if not WATCHLIST and not WHALE_WALLETS:
        log.warning(
            "WATCHLIST et WHALE_WALLETS sont vides — édite le script (ou utilise "
            "--init-config) avant de lancer une vraie surveillance. Le whale "
            "tracking fonctionnera quand même via le leaderboard automatique."
        )

    ntfy_topic = args.ntfy_topic or NTFY_TOPIC

    if args.loop:
        log.info("Mode boucle — intervalle %ds. Ctrl+C pour arrêter.", args.loop)
        try:
            while True:
                run_once(webhook_url=args.webhook, ntfy_topic=ntfy_topic)
                time.sleep(args.loop)
        except KeyboardInterrupt:
            log.info("Arrêt demandé.")
            sys.exit(0)
    else:
        run_once(webhook_url=args.webhook, ntfy_topic=ntfy_topic)


if __name__ == "__main__":
    main()