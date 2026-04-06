#!/usr/bin/env python3
"""
import_data.py — Script d'import des prospects depuis prospects.xlsx
Utilise l'API JSON exportée (plus léger que d'embarquer openpyxl en prod).

Usage:
  python import_data.py                    # Import depuis prospects_data.json
  python import_data.py --xlsx path.xlsx   # Import depuis un fichier Excel
"""
import os, sys, json

sys.path.insert(0, os.path.dirname(__file__))
from app import app, db
from models import Prospect

STATUS_MAP = {
    "🆕 À traiter": "new", "📤 Email 1 envoyé": "email_sent",
    "📤 Email 2 envoyé": "email_sent", "📤 Email 3 envoyé": "email_sent",
    "🔗 LinkedIn envoyé": "linkedin", "📧+🔗 Les deux envoyés": "both_sent",
    "↩️ Réponse reçue": "replied", "📅 RDV planifié": "meeting",
    "✅ Client converti": "converted", "❌ Pas intéressé": "not_interested",
    "💤 À recontacter": "follow_up", "🚫 Désabonné": "unsubscribed",
}


def import_from_json(json_path):
    """Import depuis le fichier JSON exporté par le script prospecting."""
    with open(json_path, "r", encoding="utf-8") as f:
        prospects = json.load(f)

    imported = 0
    skipped = 0

    for pd in prospects:
        nom = pd.get("nom", "").strip()
        if not nom:
            continue

        # Skip demo
        site = pd.get("site_web", pd.get("site", ""))
        if "demo" in str(site).lower():
            skipped += 1
            continue

        # Skip duplicates
        if Prospect.query.filter_by(nom=nom).first():
            skipped += 1
            continue

        p = Prospect(
            nom=nom,
            type=pd.get("type", ""),
            ville=pd.get("ville", ""),
            region=pd.get("region", pd.get("ville", "")),
            adresse=pd.get("adresse", ""),
            telephone=pd.get("telephone", ""),
            email=pd.get("email", pd.get("email_contact", "")),
            site_web=str(site),
            google_maps=pd.get("googleMaps", pd.get("google_maps_url", "")),
            note_google=float(pd.get("note", pd.get("note_google", 0)) or 0),
            nb_avis=int(pd.get("avis", pd.get("nb_avis", 0)) or 0),
            status=pd.get("status", "new"),
        )
        db.session.add(p)
        imported += 1

        if imported % 500 == 0:
            db.session.commit()
            print(f"  ... {imported} importés")

    db.session.commit()
    print(f"\n✅ {imported} prospects importés | {skipped} ignorés (démo/doublons)")


def import_from_xlsx(xlsx_path):
    """Import depuis le fichier Excel tracker."""
    from app import import_from_excel
    import_from_excel(xlsx_path)


if __name__ == "__main__":
    with app.app_context():
        db.create_all()

        if "--xlsx" in sys.argv:
            idx = sys.argv.index("--xlsx")
            path = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else "../prospecting/prospects.xlsx"
            print(f"📂 Import Excel: {path}")
            import_from_xlsx(path)
        else:
            # Try JSON first
            json_path = "../prospecting/prospects_data.json"
            if os.path.exists(json_path):
                print(f"📂 Import JSON: {json_path}")
                import_from_json(json_path)
            elif os.path.exists("../prospecting/prospects.xlsx"):
                print("📂 Import Excel: ../prospecting/prospects.xlsx")
                import_from_xlsx("../prospecting/prospects.xlsx")
            else:
                print("❌ Aucun fichier de données trouvé.")
                print("   Placez prospects_data.json ou prospects.xlsx dans prospecting/")
