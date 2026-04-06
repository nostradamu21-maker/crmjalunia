"""
app.py — Jalunia CRM Backend
Flask API + serves React frontend
"""
import os
import json
import smtplib
import imaplib
import email as emaillib
import ssl
import re
import secrets
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps

import hmac
import hashlib
import base64
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from models import db, Prospect, EmailLog, Setting

# ─── App Config ───────────────────────────────────────────────────────────────
JWT_EXPIRATION_HOURS = int(os.environ.get("JWT_EXPIRATION_HOURS", "24"))
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "https://crmjalunia.onrender.com").split(",")

def create_app():
    app = Flask(__name__, static_folder="static", static_url_path="")

    # Database: PostgreSQL on Railway, SQLite locally
    database_url = os.environ.get("DATABASE_URL", "sqlite:////tmp/jalunia_crm.db")
    # Railway uses postgres:// but SQLAlchemy needs postgresql://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))

    db.init_app(app)

    # CORS: restrict to allowed origins only
    CORS(app, origins=ALLOWED_ORIGINS)

    with app.app_context():
        db.create_all()

    return app

app = create_app()

# Rate limiter: prevent brute force attacks
limiter = Limiter(get_remote_address, app=app, default_limits=["200 per minute"])

# ─── Auth (JWT) ───────────────────────────────────────────────────────────────
CRM_PASSWORD = os.environ.get("CRM_PASSWORD", "jalunia2026")

def _b64url_encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

def _b64url_decode(s):
    s += "=" * (4 - len(s) % 4)
    return base64.urlsafe_b64decode(s)

def _create_token():
    """Create a signed token (HMAC-SHA256) with expiration."""
    header = _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url_encode(json.dumps({
        "sub": "crm_user",
        "iat": int(datetime.now(timezone.utc).timestamp()),
        "exp": int((datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS)).timestamp()),
    }).encode())
    sig_input = f"{header}.{payload}".encode()
    signature = _b64url_encode(hmac.new(app.config["SECRET_KEY"].encode(), sig_input, hashlib.sha256).digest())
    return f"{header}.{payload}.{signature}"

def _verify_token(token):
    """Verify token signature and expiration."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return False
        sig_input = f"{parts[0]}.{parts[1]}".encode()
        expected_sig = _b64url_encode(hmac.new(app.config["SECRET_KEY"].encode(), sig_input, hashlib.sha256).digest())
        if not hmac.compare_digest(parts[2], expected_sig):
            return False
        payload = json.loads(_b64url_decode(parts[1]))
        if payload.get("exp", 0) < datetime.now(timezone.utc).timestamp():
            return False
        return True
    except Exception:
        return False

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        token = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else ""
        if not token or not _verify_token(token):
            return jsonify({"error": "Non autorisé"}), 401
        return f(*args, **kwargs)
    return decorated

# ─── API: Auth ────────────────────────────────────────────────────────────────
@app.route("/api/login", methods=["POST"])
@limiter.limit("5 per minute")
def login():
    data = request.get_json() or {}
    if data.get("password") == CRM_PASSWORD:
        token = _create_token()
        return jsonify({"token": token, "ok": True})
    return jsonify({"error": "Mot de passe incorrect"}), 401

# ─── API: Prospects ───────────────────────────────────────────────────────────
@app.route("/api/prospects")
@require_auth
def get_prospects():
    """Liste paginée des prospects avec filtres."""
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    search = request.args.get("search", "").strip()
    status = request.args.get("status", "")
    type_ = request.args.get("type", "")
    ville = request.args.get("ville", "")
    has_email = request.args.get("has_email", "")

    q = Prospect.query

    if search:
        like = f"%{search}%"
        q = q.filter(
            db.or_(
                Prospect.nom.ilike(like),
                Prospect.ville.ilike(like),
                Prospect.email.ilike(like),
                Prospect.region.ilike(like),
            )
        )
    if status:
        q = q.filter_by(status=status)
    if type_:
        q = q.filter_by(type=type_)
    if ville:
        q = q.filter_by(ville=ville)
    if has_email == "with":
        q = q.filter(Prospect.email != "", Prospect.email.isnot(None))
    elif has_email == "without":
        q = q.filter(db.or_(Prospect.email == "", Prospect.email.is_(None)))

    q = q.order_by(Prospect.date_ajout.desc())
    total = q.count()
    prospects = q.offset((page - 1) * per_page).limit(per_page).all()

    return jsonify({
        "prospects": [p.to_dict() for p in prospects],
        "total": total,
        "page": page,
        "pages": (total + per_page - 1) // per_page,
    })

@app.route("/api/prospects/stats")
@require_auth
def get_stats():
    """KPIs et statistiques globales."""
    total = Prospect.query.count()
    with_email = Prospect.query.filter(Prospect.email != "", Prospect.email.isnot(None)).count()

    status_counts = {}
    for row in db.session.query(Prospect.status, db.func.count(Prospect.id)).group_by(Prospect.status).all():
        status_counts[row[0]] = row[1]

    type_counts = {}
    for row in db.session.query(Prospect.type, db.func.count(Prospect.id)).group_by(Prospect.type).order_by(db.func.count(Prospect.id).desc()).all():
        type_counts[row[0]] = row[1]

    ville_counts = {}
    for row in db.session.query(Prospect.ville, db.func.count(Prospect.id)).group_by(Prospect.ville).order_by(db.func.count(Prospect.id).desc()).limit(20).all():
        ville_counts[row[0]] = row[1]

    # Email coverage by type
    email_by_type = {}
    for t, c in type_counts.items():
        with_e = Prospect.query.filter(Prospect.type == t, Prospect.email != "", Prospect.email.isnot(None)).count()
        email_by_type[t] = {"total": c, "withEmail": with_e, "pct": round(with_e / c * 100) if c > 0 else 0}

    contacted = total - status_counts.get("new", 0)
    replied = sum(status_counts.get(s, 0) for s in ["replied", "meeting", "converted"])

    return jsonify({
        "total": total,
        "withEmail": with_email,
        "contacted": contacted,
        "replied": replied,
        "meetings": status_counts.get("meeting", 0),
        "converted": status_counts.get("converted", 0),
        "statusCounts": status_counts,
        "typeCounts": type_counts,
        "villeCounts": ville_counts,
        "emailByType": email_by_type,
    })

@app.route("/api/prospects/<int:pid>")
@require_auth
def get_prospect(pid):
    p = Prospect.query.get_or_404(pid)
    data = p.to_dict()
    data["emailLogs"] = [l.to_dict() for l in p.email_logs.order_by(EmailLog.sent_at.desc()).all()]
    return jsonify(data)

@app.route("/api/prospects/<int:pid>", methods=["PATCH"])
@require_auth
def update_prospect(pid):
    p = Prospect.query.get_or_404(pid)
    data = request.get_json() or {}

    field_map = {
        "nom": "nom", "type": "type", "ville": "ville", "region": "region",
        "email": "email", "telephone": "telephone", "site": "site_web",
        "notes": "notes", "status": "status", "adresse": "adresse",
        "linkedinUrl": "linkedin_url",
    }
    for json_key, db_key in field_map.items():
        if json_key in data:
            setattr(p, db_key, data[json_key])

    db.session.commit()
    return jsonify(p.to_dict())

@app.route("/api/prospects", methods=["POST"])
@require_auth
def create_prospect():
    data = request.get_json() or {}
    if not data.get("nom"):
        return jsonify({"error": "Nom requis"}), 400

    p = Prospect(
        nom=data["nom"],
        type=data.get("type", "hébergement"),
        ville=data.get("ville", ""),
        region=data.get("region", data.get("ville", "")),
        email=data.get("email", ""),
        telephone=data.get("telephone", ""),
        site_web=data.get("site", ""),
        notes=data.get("notes", ""),
        status="new",
    )
    db.session.add(p)
    db.session.commit()
    return jsonify(p.to_dict()), 201

@app.route("/api/prospects/<int:pid>", methods=["DELETE"])
@require_auth
def delete_prospect(pid):
    p = Prospect.query.get_or_404(pid)
    EmailLog.query.filter_by(prospect_id=pid).delete()
    db.session.delete(p)
    db.session.commit()
    return jsonify({"ok": True})

# ─── API: Filters (unique values) ────────────────────────────────────────────
@app.route("/api/filters")
@require_auth
def get_filters():
    types = [r[0] for r in db.session.query(Prospect.type).distinct().order_by(Prospect.type).all() if r[0]]
    villes = [r[0] for r in db.session.query(Prospect.ville, db.func.count(Prospect.id)).group_by(Prospect.ville).order_by(db.func.count(Prospect.id).desc()).limit(50).all() if r[0]]
    return jsonify({"types": types, "villes": villes})

# ─── API: Email Sending ──────────────────────────────────────────────────────
@app.route("/api/send-email", methods=["POST"])
@require_auth
def send_email():
    """Envoie un email à un prospect via SMTP."""
    data = request.get_json() or {}
    pid = data.get("prospectId")
    subject = data.get("subject", "")
    body = data.get("body", "")
    email_num = data.get("emailNum", 1)

    p = Prospect.query.get_or_404(pid)
    if not p.email:
        return jsonify({"error": "Ce prospect n'a pas d'adresse email"}), 400

    # Get SMTP settings
    smtp_host = Setting.get("smtp_host", "")
    smtp_port = int(Setting.get("smtp_port", "587"))
    smtp_user = Setting.get("smtp_user", "")
    smtp_pass = Setting.get("smtp_pass", "")
    sender_email = Setting.get("sender_email", smtp_user)
    sender_name = Setting.get("sender_name", "Simon Stoll — Jalunia")

    if not smtp_host or not smtp_user:
        return jsonify({"error": "SMTP non configuré. Allez dans Paramètres."}), 400

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = p.email
        msg["Reply-To"] = sender_email

        # Plain text
        msg.attach(MIMEText(body, "plain", "utf-8"))
        # HTML version
        html_body = body.replace("\n", "<br>")
        msg.attach(MIMEText(f"<html><body style='font-family:Arial,sans-serif;font-size:14px;color:#333;line-height:1.6'>{html_body}</body></html>", "html", "utf-8"))

        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

        # Log the email
        log = EmailLog(
            prospect_id=p.id,
            email_num=email_num,
            subject=subject,
            body=body,
            status="sent",
        )
        db.session.add(log)

        # Update prospect status
        p.emails_sent = max(p.emails_sent or 0, email_num)
        p.last_email_date = datetime.now(timezone.utc)
        p.date_contact_email = p.date_contact_email or datetime.now(timezone.utc)
        if p.status == "new":
            p.status = "email_sent"

        db.session.commit()
        return jsonify({"ok": True, "message": f"Email envoyé à {p.email}"})

    except Exception as e:
        return jsonify({"error": f"Erreur SMTP: {str(e)}"}), 500

# ─── API: Check Inbox (IMAP) ─────────────────────────────────────────────────
@app.route("/api/check-inbox", methods=["POST"])
@require_auth
def check_inbox():
    """Vérifie la boîte mail pour les réponses et les STOP."""
    imap_host = Setting.get("imap_host", "")
    imap_port = int(Setting.get("imap_port", "993"))
    imap_user = Setting.get("imap_user", Setting.get("smtp_user", ""))
    imap_pass = Setting.get("imap_pass", Setting.get("smtp_pass", ""))

    if not imap_host or not imap_user:
        return jsonify({"error": "IMAP non configuré. Allez dans Paramètres."}), 400

    results = {"replies": 0, "stops": 0, "errors": 0}

    try:
        mail = imaplib.IMAP4_SSL(imap_host, imap_port)
        mail.login(imap_user, imap_pass)
        mail.select("INBOX")

        # Search for recent unseen messages
        _, message_ids = mail.search(None, "UNSEEN")

        for mid in message_ids[0].split():
            try:
                _, msg_data = mail.fetch(mid, "(RFC822)")
                msg = emaillib.message_from_bytes(msg_data[0][1])

                from_addr = emaillib.utils.parseaddr(msg.get("From", ""))[1].lower()
                subject = str(emaillib.header.decode_header(msg.get("Subject", ""))[0][0] or "")
                if isinstance(subject, bytes):
                    subject = subject.decode("utf-8", errors="ignore")

                # Get body
                body_text = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body_text = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                            break
                else:
                    body_text = msg.get_payload(decode=True).decode("utf-8", errors="ignore")

                # Find matching prospect
                prospect = Prospect.query.filter(db.func.lower(Prospect.email) == from_addr).first()
                if not prospect:
                    continue

                # Check for STOP
                if re.search(r'\bSTOP\b', body_text, re.IGNORECASE):
                    prospect.status = "unsubscribed"
                    results["stops"] += 1
                else:
                    if prospect.status in ("email_sent", "linkedin", "both_sent"):
                        prospect.status = "replied"
                    results["replies"] += 1

                # Log the reply
                log = EmailLog.query.filter_by(prospect_id=prospect.id).order_by(EmailLog.sent_at.desc()).first()
                if log:
                    log.status = "replied"
                    log.reply_body = body_text[:2000]
                    log.reply_at = datetime.now(timezone.utc)

                db.session.commit()

            except Exception:
                results["errors"] += 1

        mail.logout()
        return jsonify({"ok": True, **results})

    except Exception as e:
        return jsonify({"error": f"Erreur IMAP: {str(e)}"}), 500

# ─── API: Settings ────────────────────────────────────────────────────────────
@app.route("/api/settings", methods=["GET"])
@require_auth
def get_settings():
    keys = ["smtp_host", "smtp_port", "smtp_user", "smtp_pass", "sender_email",
            "sender_name", "imap_host", "imap_port", "imap_user", "imap_pass",
            "daily_limit", "delay_email2", "delay_email3"]
    return jsonify({k: Setting.get(k, "") for k in keys})

@app.route("/api/settings", methods=["POST"])
@require_auth
def save_settings():
    data = request.get_json() or {}
    for key, value in data.items():
        Setting.set(key, str(value))
    return jsonify({"ok": True})

# ─── API: Bulk Actions ────────────────────────────────────────────────────────
@app.route("/api/bulk-status", methods=["POST"])
@require_auth
def bulk_status():
    """Change le statut de plusieurs prospects d'un coup."""
    data = request.get_json() or {}
    ids = data.get("ids", [])
    new_status = data.get("status", "")
    if not ids or not new_status:
        return jsonify({"error": "ids et status requis"}), 400

    Prospect.query.filter(Prospect.id.in_(ids)).update({Prospect.status: new_status}, synchronize_session=False)
    db.session.commit()
    return jsonify({"ok": True, "updated": len(ids)})

# ─── API: Import from Excel ──────────────────────────────────────────────────
@app.route("/api/import", methods=["POST"])
@require_auth
def import_prospects():
    """Import CSV/JSON data."""
    data = request.get_json() or {}
    prospects_data = data.get("prospects", [])
    imported = 0

    for pd in prospects_data:
        if not pd.get("nom"):
            continue
        existing = Prospect.query.filter_by(nom=pd["nom"]).first()
        if existing:
            continue
        p = Prospect(
            nom=pd["nom"], type=pd.get("type", ""), ville=pd.get("ville", ""),
            region=pd.get("region", pd.get("ville", "")),
            email=pd.get("email", ""), telephone=pd.get("telephone", ""),
            site_web=pd.get("site", ""), note_google=pd.get("note", 0),
            nb_avis=pd.get("avis", 0), status=pd.get("status", "new"),
            adresse=pd.get("adresse", ""), google_maps=pd.get("googleMaps", ""),
        )
        db.session.add(p)
        imported += 1

    db.session.commit()
    return jsonify({"ok": True, "imported": imported})

# ─── Health Check ────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    """Health check endpoint for Render monitoring."""
    try:
        db.session.execute(db.text("SELECT 1"))
        return jsonify({"status": "healthy", "database": "connected"}), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "database": str(e)}), 503

# ─── Serve Frontend ──────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/<path:path>")
def static_files(path):
    return send_from_directory("static", path)

# ─── Init Script: Import from prospects.xlsx ──────────────────────────────────
def import_from_excel(xlsx_path):
    """Import prospects from the existing Excel tracker."""
    import openpyxl

    STATUS_MAP = {
        "🆕 À traiter": "new", "📤 Email 1 envoyé": "email_sent",
        "📤 Email 2 envoyé": "email_sent", "📤 Email 3 envoyé": "email_sent",
        "🔗 LinkedIn envoyé": "linkedin", "📧+🔗 Les deux envoyés": "both_sent",
        "↩️ Réponse reçue": "replied", "📅 RDV planifié": "meeting",
        "✅ Client converti": "converted", "❌ Pas intéressé": "not_interested",
        "💤 À recontacter": "follow_up", "🚫 Désabonné": "unsubscribed",
    }

    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb[wb.sheetnames[0]]

    # Skip demo prospects (those with "demo" in the site URL)
    imported = 0
    skipped_demo = 0
    skipped_dup = 0

    for r in range(2, ws.max_row + 1):
        nom = ws.cell(r, 1).value
        if not nom:
            continue
        nom = str(nom).strip()

        site = str(ws.cell(r, 8).value or "").strip()
        # Skip demo prospects
        if "demo" in site.lower() or nom.endswith((" 1", " 2", " 3", " 4", " 5", " 6", " 7", " 8", " 9", " 10")):
            # Check if it's actually a demo by looking at the site URL
            if "demo" in site.lower():
                skipped_demo += 1
                continue

        # Skip duplicates
        existing = Prospect.query.filter_by(nom=nom).first()
        if existing:
            skipped_dup += 1
            continue

        status_raw = ws.cell(r, 13).value or "🆕 À traiter"
        status = STATUS_MAP.get(status_raw, "new")

        da = ws.cell(r, 14).value
        dc = ws.cell(r, 15).value

        p = Prospect(
            nom=nom,
            type=str(ws.cell(r, 2).value or "").strip(),
            ville=str(ws.cell(r, 3).value or "").strip(),
            region=str(ws.cell(r, 4).value or "").strip(),
            adresse=str(ws.cell(r, 5).value or "").strip(),
            telephone=str(ws.cell(r, 6).value or "").strip(),
            email=str(ws.cell(r, 7).value or "").strip(),
            site_web=site,
            google_maps=str(ws.cell(r, 9).value or "").strip(),
            note_google=float(ws.cell(r, 10).value or 0),
            nb_avis=int(ws.cell(r, 11).value or 0),
            status=status,
            date_ajout=da if hasattr(da, "strftime") else datetime.now(timezone.utc),
            date_contact_email=dc if hasattr(dc, "strftime") else None,
            email1_sujet=str(ws.cell(r, 18).value or "").strip(),
            email1_corps=str(ws.cell(r, 19).value or "").strip(),
            email2_sujet=str(ws.cell(r, 20).value or "").strip(),
            email2_corps=str(ws.cell(r, 21).value or "").strip(),
            email3_sujet=str(ws.cell(r, 22).value or "").strip(),
            email3_corps=str(ws.cell(r, 23).value or "").strip(),
            linkedin_connexion=str(ws.cell(r, 24).value or "").strip(),
            linkedin_msg1=str(ws.cell(r, 25).value or "").strip(),
            linkedin_msg2=str(ws.cell(r, 26).value or "").strip(),
            emails_sent=1 if "Email" in status_raw else 0,
        )
        db.session.add(p)
        imported += 1

        if imported % 500 == 0:
            db.session.commit()
            print(f"  ... {imported} importés")

    db.session.commit()
    print(f"\n✅ Import terminé: {imported} prospects importés | {skipped_demo} démo ignorés | {skipped_dup} doublons ignorés")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "import":
        xlsx_path = sys.argv[2] if len(sys.argv) > 2 else "../prospecting/prospects.xlsx"
        with app.app_context():
            print(f"📂 Import depuis {xlsx_path}...")
            import_from_excel(xlsx_path)
    else:
        port = int(os.environ.get("PORT", 5000))
        app.run(host="0.0.0.0", port=port, debug=os.environ.get("FLASK_DEBUG", "0") == "1")
