"""
models.py - Database models for Jalunia CRM
"""
from datetime import datetime, timezone
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Prospect(db.Model):
    __tablename__ = "prospects"

    id = db.Column(db.Integer, primary_key=True)
    nom = db.Column(db.String(200), nullable=False, index=True)
    type = db.Column(db.String(80), default="hebergement")
    ville = db.Column(db.String(100), index=True)
    region = db.Column(db.String(100))
    adresse = db.Column(db.String(300))
    telephone = db.Column(db.String(30))
    email = db.Column(db.String(200), index=True)
    site_web = db.Column(db.String(300))
    google_maps = db.Column(db.String(500))
    note_google = db.Column(db.Float, default=0)
    nb_avis = db.Column(db.Integer, default=0)
    linkedin_url = db.Column(db.String(300))

    status = db.Column(db.String(30), default="new", index=True)
    date_ajout = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    date_contact_email = db.Column(db.DateTime, nullable=True)
    date_contact_linkedin = db.Column(db.DateTime, nullable=True)
    notes = db.Column(db.Text, default="")
    bounce_count = db.Column(db.Integer, default=0)

    # Scoring & tracking
    score = db.Column(db.Integer, default=0, index=True)
    email_opened = db.Column(db.Boolean, default=False)
    unsubscribe_token = db.Column(db.String(64), unique=True, index=True, nullable=True)

    # Pre-generated messages
    email1_sujet = db.Column(db.String(300))
    email1_corps = db.Column(db.Text)
    email2_sujet = db.Column(db.String(300))
    email2_corps = db.Column(db.Text)
    email3_sujet = db.Column(db.String(300))
    email3_corps = db.Column(db.Text)
    linkedin_connexion = db.Column(db.Text)
    linkedin_msg1 = db.Column(db.Text)
    linkedin_msg2 = db.Column(db.Text)

    # Track which emails have been sent
    emails_sent = db.Column(db.Integer, default=0)
    last_email_date = db.Column(db.DateTime, nullable=True)

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    def to_dict(self):
        return {
            "id": self.id,
            "nom": self.nom,
            "type": self.type,
            "ville": self.ville,
            "region": self.region or self.ville,
            "adresse": self.adresse or "",
            "telephone": self.telephone or "",
            "email": self.email or "",
            "site": self.site_web or "",
            "googleMaps": self.google_maps or "",
            "note": self.note_google or 0,
            "avis": self.nb_avis or 0,
            "linkedinUrl": self.linkedin_url or "",
            "status": self.status,
            "dateAjout": self.date_ajout.isoformat() if self.date_ajout else "",
            "dateContact": self.date_contact_email.isoformat() if self.date_contact_email else "",
            "notes": self.notes or "",
            "emailsSent": self.emails_sent or 0,
            "lastEmailDate": self.last_email_date.isoformat() if self.last_email_date else "",
            "bounceCount": self.bounce_count or 0,
            "email1Sujet": self.email1_sujet or "",
            "email1Corps": self.email1_corps or "",
            "email2Sujet": self.email2_sujet or "",
            "email2Corps": self.email2_corps or "",
            "email3Sujet": self.email3_sujet or "",
            "email3Corps": self.email3_corps or "",
        }


class EmailLog(db.Model):
    __tablename__ = "email_logs"

    id = db.Column(db.Integer, primary_key=True)
    prospect_id = db.Column(db.Integer, db.ForeignKey("prospects.id"), nullable=False, index=True)
    email_num = db.Column(db.Integer)
    subject = db.Column(db.String(300))
    body = db.Column(db.Text)
    sent_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    status = db.Column(db.String(20), default="sent")  # sent, bounced, replied
    error_message = db.Column(db.Text, nullable=True)
    reply_body = db.Column(db.Text, nullable=True)
    reply_at = db.Column(db.DateTime, nullable=True)

    # Open tracking
    tracking_id = db.Column(db.String(64), unique=True, index=True, nullable=True)
    opened_at = db.Column(db.DateTime, nullable=True)
    open_count = db.Column(db.Integer, default=0)

    prospect = db.relationship("Prospect", backref=db.backref("email_logs", lazy="dynamic"))

    def to_dict(self):
        return {
            "id": self.id,
            "prospectId": self.prospect_id,
            "emailNum": self.email_num,
            "subject": self.subject,
            "sentAt": self.sent_at.isoformat() if self.sent_at else "",
            "status": self.status,
            "errorMessage": self.error_message or "",
            "replyBody": self.reply_body or "",
            "replyAt": self.reply_at.isoformat() if self.reply_at else "",
            "openedAt": self.opened_at.isoformat() if self.opened_at else "",
            "openCount": self.open_count or 0,
        }


class CampaignRun(db.Model):
    __tablename__ = "campaign_runs"

    id = db.Column(db.Integer, primary_key=True)
    started_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = db.Column(db.DateTime, nullable=True)
    emails_sent = db.Column(db.Integer, default=0)
    emails_failed = db.Column(db.Integer, default=0)
    bounces_detected = db.Column(db.Integer, default=0)
    replies_detected = db.Column(db.Integer, default=0)
    stops_detected = db.Column(db.Integer, default=0)
    status = db.Column(db.String(20), default="running")
    details = db.Column(db.Text)

    def to_dict(self):
        return {
            "id": self.id,
            "startedAt": self.started_at.isoformat() if self.started_at else "",
            "finishedAt": self.finished_at.isoformat() if self.finished_at else "",
            "emailsSent": self.emails_sent,
            "emailsFailed": self.emails_failed,
            "bouncesDetected": self.bounces_detected,
            "repliesDetected": self.replies_detected,
            "stopsDetected": self.stops_detected,
            "status": self.status,
        }


class CampaignRun(db.Model):
    __tablename__ = "campaign_runs"

    id = db.Column(db.Integer, primary_key=True)
    started_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = db.Column(db.DateTime, nullable=True)
    emails_sent = db.Column(db.Integer, default=0)
    emails_failed = db.Column(db.Integer, default=0)
    bounces_detected = db.Column(db.Integer, default=0)
    replies_detected = db.Column(db.Integer, default=0)
    stops_detected = db.Column(db.Integer, default=0)
    status = db.Column(db.String(20), default="running")  # running, completed, error
    details = db.Column(db.Text)  # JSON with per-email results

    def to_dict(self):
        return {
            "id": self.id,
            "startedAt": self.started_at.isoformat() if self.started_at else "",
            "finishedAt": self.finished_at.isoformat() if self.finished_at else "",
            "emailsSent": self.emails_sent,
            "emailsFailed": self.emails_failed,
            "bouncesDetected": self.bounces_detected,
            "repliesDetected": self.replies_detected,
            "stopsDetected": self.stops_detected,
            "status": self.status,
        }


class Setting(db.Model):
    __tablename__ = "settings"

    key = db.Column(db.String(100), primary_key=True)
    value = db.Column(db.Text)

    @staticmethod
    def get(key, default=""):
        s = Setting.query.get(key)
        return s.value if s else default

    @staticmethod
    def set(key, value):
        s = Setting.query.get(key)
        if s:
            s.value = value
        else:
            s = Setting(key=key, value=value)
            db.session.add(s)
        db.session.commit()
