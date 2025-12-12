import pandas as pd
from datetime import datetime
import os
import re
from weasyprint import HTML, CSS
import io
import urllib.request
import cloudinary
import cloudinary.uploader
import cloudinary.api
import requests
import json
import sys

# ==============================
# ⚙️ CONFIG CLOUDINARY
# ==============================
CLOUDINARY_CONFIG = {
    "cloud_name": "dyfbqfodx",
    "api_key": "344423916816885",
    "api_secret": "PxY778LiaKA8EAhO8l-Q_Jiw7-M",
    "secure": True
}

# Dossiers sur Cloudinary
CLOUDINARY_BASE_FOLDER = "factures"
CLOUDINARY_BDD_FOLDER = "BDD"  # Dossier où se trouve donnees.xlsx

# ==============================
# ⚙️ CONFIG LOCALE
# ==============================
FICHIER_EXCEL = "donnees.xlsx"
DOSSIER_SORTIE = "factures"
DOSSIER_HTML = "factures_html"
DOSSIER_TEMP = "temp_factures"
DOSSIER_HTML_TEMP = "temp_html"

os.makedirs(DOSSIER_SORTIE, exist_ok=True)
os.makedirs(DOSSIER_HTML, exist_ok=True)
os.makedirs(DOSSIER_TEMP, exist_ok=True)
os.makedirs(DOSSIER_HTML_TEMP, exist_ok=True)

# ==============================
# 📊 PROGRESS TRACKING
# ==============================
class ProgressTracker:
    def __init__(self):
        self.current_step = 0
        self.total_steps = 0
        self.status = "idle"
        self.message = ""
        self.progress = 0
        self.result_urls = []
        self.error = None
        self.start_time = None
        self.end_time = None
    
    def update(self, step, total_steps, status, message, progress=None):
        self.current_step = step
        self.total_steps = total_steps
        self.status = status
        self.message = message
        
        if progress is not None:
            self.progress = progress
        elif total_steps > 0:
            self.progress = min(100, int((step / total_steps) * 100))
        
        # Envoyer la progression via stdout
        progress_data = {
            "step": step,
            "total_steps": total_steps,
            "status": status,
            "message": message,
            "progress": self.progress,
            "urls": self.result_urls,
            "error": self.error
        }
        print(f"PROGRESS:{json.dumps(progress_data)}", flush=True)
    
    def add_result_url(self, url_info):
        self.result_urls.append(url_info)
    
    def set_error(self, error_message):
        self.error = error_message
        self.status = "error"
        progress_data = {
            "step": self.current_step,
            "total_steps": self.total_steps,
            "status": "error",
            "message": self.message,
            "progress": self.progress,
            "urls": self.result_urls,
            "error": self.error
        }
        print(f"PROGRESS:{json.dumps(progress_data)}", flush=True)

# Créer un tracker global
tracker = ProgressTracker()

# Fonction pour envoyer la progression (format JSON sur stdout)
def send_progress(step, total_steps, status, message, progress=None, urls=None, error=None):
    """Envoie les données de progression au format JSON"""
    if progress is None and total_steps > 0:
        progress = min(100, int((step / total_steps) * 100))
    progress_data = {
        "step": step,
        "total_steps": total_steps,
        "status": status,
        "message": message,
        "progress": progress,
        "urls": urls or [],
        "error": error
    }
    print(f"PROGRESS:{json.dumps(progress_data)}", flush=True)
    sys.stdout.flush()

def send_summary(total_clients, factures_generees, duree, mois_annee):
    """Envoie le résumé final"""
    summary_data = {
        "total_clients": total_clients,
        "factures_generees": factures_generees,
        "duree": duree,
        "mois_annee": mois_annee
    }
    print(f"SUMMARY:{json.dumps(summary_data)}", flush=True)
    sys.stdout.flush()

# ==============================
# 🔍 CHARGEMENT DES DONNÉES
# ==============================

# 🔧 INITIALISATION CLOUDINARY
# ==============================
def initialiser_cloudinary():
    """Initialise la configuration Cloudinary"""
    try:
        cloudinary.config(
            cloud_name=CLOUDINARY_CONFIG["cloud_name"],
            api_key=CLOUDINARY_CONFIG["api_key"],
            api_secret=CLOUDINARY_CONFIG["api_secret"],
            secure=CLOUDINARY_CONFIG["secure"]
        )
        print("✅ Cloudinary initialisé avec succès")
        tracker.update(1, 5, "initializing", "Cloudinary initialisé")
        return True
    except Exception as e:
        print(f"❌ Erreur d'initialisation Cloudinary: {e}")
        tracker.set_error(f"Erreur d'initialisation Cloudinary: {e}")
        return False

# ==============================
# ☁️ FONCTIONS CLOUDINARY
# ==============================
def telecharger_excel_depuis_cloudinary():
    """
    Télécharge le fichier Excel depuis Cloudinary en utilisant l'API de recherche.
    """
    try:
        print(f"📥 Recherche du fichier Excel dans le dossier 'BDD'...")
        tracker.update(2, 5, "loading", "Recherche fichier Excel sur Cloudinary")
        
        # Utiliser l'API de recherche pour trouver des fichiers bruts dans le dossier 'BDD'
        resultats = cloudinary.Search() \
            .expression("resource_type:raw AND asset_folder=BDD") \
            .max_results(50) \
            .execute()
        
        if 'resources' in resultats and resultats['resources']:
            # Chercher un fichier Excel parmi les résultats
            fichiers_excel = []
            for res in resultats['resources']:
                # Vérifier par format ou nom de fichier
                if res.get('format') in ['xlsx', 'xls'] or 'donnees' in res.get('public_id', '').lower():
                    fichiers_excel.append(res)
                    print(f"   📄 Fichier trouvé: {res.get('public_id')} (dans le dossier 'BDD')")
            
            if not fichiers_excel:
                print("❌ Aucun fichier Excel trouvé dans le dossier 'BDD' via la recherche.")
                return None
            
            # Prendre le premier fichier Excel trouvé
            fichier_excel = fichiers_excel[0]
            public_id = fichier_excel['public_id']
            print(f"✅ Sélection du fichier: {public_id}")
            
            # Générer l'URL de téléchargement sécurisée pour un fichier brut
            url_fichier, _ = cloudinary.utils.cloudinary_url(
                public_id,
                resource_type="raw",
                type="upload",
                secure=True
            )
            
            print(f"📥 Téléchargement depuis: {url_fichier}")
            
            # Télécharger le fichier
            response = requests.get(url_fichier)
            response.raise_for_status()
            
            # Lire le fichier Excel avec pandas
            excel_data = io.BytesIO(response.content)
            
            # Détecter si c'est un .xlsx ou .xls
            if fichier_excel.get('format') == 'xls':
                df_complet = pd.read_excel(excel_data, header=None, engine='xlrd')
            else:
                df_complet = pd.read_excel(excel_data, header=None)
            
            print(f"✅ Fichier Excel téléchargé avec succès depuis Cloudinary ({len(df_complet)} lignes)")
            return df_complet
            
        else:
            print(f"❌ Aucun fichier trouvé dans le dossier 'BDD' via la recherche.")
            return None
            
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement Excel depuis Cloudinary: {e}")
        import traceback
        traceback.print_exc()
        tracker.set_error(f"Erreur téléchargement Excel: {e}")
        return None

def uploader_vers_cloudinary(chemin_fichier, nom_client, mois_annee=None, sous_dossier=""):
    """
    Téléverse un fichier vers Cloudinary
    
    Args:
        chemin_fichier: Chemin local du fichier
        nom_client: Nom du client pour le dossier
        mois_annee: Mois/année pour l'organisation (optionnel)
        sous_dossier: Sous-dossier supplémentaire (ex: "html", "pdf")
    """
    try:
        # Nettoyer le nom du client pour le dossier
        nom_client_propre = re.sub(r'[<>:"/\\|?*]', '_', str(nom_client))
        nom_client_propre = nom_client_propre.replace(' ', '_')
        
        # Construire le chemin Cloudinary
        if sous_dossier:
            if mois_annee:
                cloudinary_path = f"{CLOUDINARY_BASE_FOLDER}/{mois_annee}/{sous_dossier}/{nom_client_propre}"
            else:
                cloudinary_path = f"{CLOUDINARY_BASE_FOLDER}/{sous_dossier}/{nom_client_propre}"
        else:
            if mois_annee:
                cloudinary_path = f"{CLOUDINARY_BASE_FOLDER}/{mois_annee}/{nom_client_propre}"
            else:
                cloudinary_path = f"{CLOUDINARY_BASE_FOLDER}/{nom_client_propre}"
        
        # Extraire le nom du fichier
        nom_fichier = os.path.basename(chemin_fichier)
        
        # Déterminer le type de ressource
        extension = os.path.splitext(nom_fichier)[1].lower()
        resource_type = "auto"
        
        if extension == '.pdf':
            resource_type = "raw"  # Cloudinary traite les PDF comme raw
        elif extension == '.html':
            resource_type = "raw"
        elif extension == '.xlsx' or extension == '.xls':
            resource_type = "raw"
        
        # Téléverser sur Cloudinary
        print(f"   ☁️  Téléversement sur Cloudinary: {nom_fichier}")
        
        resultat = cloudinary.uploader.upload(
            chemin_fichier,
            folder=cloudinary_path,
            resource_type=resource_type,
            public_id=f"facture_{nom_client_propre}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            overwrite=True,
            tags=["facture", nom_client_propre, "clar_services", mois_annee if mois_annee else "sans_date"]
        )
        
        print(f"   ✅ Fichier téléversé avec succès")
        
        # Ajouter l'URL au tracker
        tracker.add_result_url({
            'client': nom_client,
            'url': resultat.get('secure_url'),
            'type': sous_dossier if sous_dossier else 'document'
        })
        
        # Supprimer le fichier temporaire local après téléversement
        if os.path.exists(chemin_fichier):
            os.remove(chemin_fichier)
            print(f"   🗑️  Fichier temporaire supprimé: {nom_fichier}")
        
        return {
            'success': True,
            'url': resultat.get('secure_url'),
            'public_id': resultat.get('public_id'),
            'client': nom_client,
            'folder': cloudinary_path
        }
        
    except Exception as e:
        print(f"   ❌ Erreur téléversement Cloudinary: {e}")
        tracker.set_error(f"Erreur upload {nom_client}: {e}")
        # Ne pas supprimer en cas d'erreur
        return {'success': False, 'error': str(e)}

# ==============================
# 🔍 CHARGEMENT DES DONNÉES
# ==============================

def charger_donnees():
    """Charge et nettoie les données Excel"""
    print("🔍 Recherche de la position des données...")
    tracker.update(3, 5, "loading", "Chargement des données")
    
    # Résoudre le fichier de données (priorité: Cloudinary > BDD/donnees.xlsx > DATA_URL > défaut)
    def _download_file(url, dest_path):
        try:
            print(f"🔽 Téléchargement depuis: {url}")
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            urllib.request.urlretrieve(url, dest_path)
            print(f"✅ Téléchargé vers: {dest_path}")
            return dest_path
        except Exception as e:
            print(f"❌ Erreur téléchargement: {e}")
            return None

    def _resolve_data_file():
        # Essayer Cloudinary en priorité (sans réinitialiser)
        try:
            df_cloud = telecharger_excel_depuis_cloudinary()
            if df_cloud is not None:
                return "cloudinary", df_cloud
        except Exception as e:
            print(f"⚠️  Pas de fichier Cloudinary trouvé: {e}")
        
        # Fallback sur fichier local
        data_url = os.environ.get('DATA_URL')
        bdd_path = os.path.join('BDD', 'donnees.xlsx')
        
        if data_url:
            dl = _download_file(data_url, bdd_path)
            if dl:
                return "url", dl
        
        if os.path.exists(bdd_path):
            print(f"ℹ️ Utilisation du fichier local: {bdd_path}")
            return "local", bdd_path
        
        if os.path.exists(FICHIER_EXCEL):
            print(f"ℹ️ Utilisation du fichier local: {FICHIER_EXCEL}")
            return "local", FICHIER_EXCEL
        
        print(f"⚠️ Aucun fichier de données trouvé, utilisation par défaut: {FICHIER_EXCEL}")
        return "default", FICHIER_EXCEL

    source, fichier_a_lire = _resolve_data_file()
    
    # Si c'est un DataFrame (de Cloudinary), l'utiliser directement
    if isinstance(fichier_a_lire, pd.DataFrame):
        df_complet = fichier_a_lire
    else:
        # Sinon, lire le fichier Excel
        df_complet = pd.read_excel(fichier_a_lire, header=None)
    
    ligne_titre = None
    for i in range(len(df_complet)):
        for j in range(len(df_complet.columns)):
            valeur_cellule = str(df_complet.iloc[i, j]).strip()
            if valeur_cellule.lower() == "noms":
                ligne_titre = i
                print(f"✅ 'Noms' trouvé à la position : Ligne {i}")
                break
        if ligne_titre is not None:
            break
    
    if ligne_titre is None:
        print("❌ 'Noms' non trouvé, utilisation ligne 9 par défaut")
        ligne_titre = 9

    # Charger les données
    # Si c'est un DataFrame depuis Cloudinary, l'utiliser directement
    if isinstance(fichier_a_lire, pd.DataFrame):
        df = fichier_a_lire.iloc[ligne_titre:].reset_index(drop=True)
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)
    else:
        df = pd.read_excel(fichier_a_lire, skiprows=ligne_titre, header=0)
    
    # Nettoyer les colonnes
    colonnes_a_garder = []
    for col in df.columns:
        col_str = str(col)
        if 'unnamed' not in col_str.lower() and not col_str.startswith('Unnamed'):
            colonnes_a_garder.append(col)
    
    df = df[colonnes_a_garder]
    
    # Nettoyer les noms de colonnes
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace('\xa0', ' ')
        .str.replace(' ', '_')
        .str.replace('é', 'e')
        .str.replace('è', 'e')
        .str.replace('à', 'a')
        .str.replace('(', '')
        .str.replace(')', '')
        .str.replace('-', '_')
        .str.replace('__', '_')
    )
    
    # Supprimer les lignes vides
    df = df.dropna(subset=['noms'])
    df = df.reset_index(drop=True)
    
    print(f"📊 {len(df)} clients trouvés")
    return df

# ==============================
# 🎯 NOUVEAU TEMPLATE HTML OPTIMISÉ
# ==============================

HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
body {
  margin:0;
  padding: 20px;
  font-family: Arial, sans-serif;
  background-color: #f5f5f5;
  font-size: 13px; /* réduit d'environ 3px par rapport au défaut */
}
.facture-container {
  width: 85%;
  margin: 0 auto;
  background-color: white;
  padding: 36px; /* légèrement réduit */
  box-shadow: 0 0 10px rgba(0,0,0,0.1);
}
.header-title {
  border: 3px solid #333;
  padding: 12px;
  text-align: center;
  margin-bottom: 18px;
  background-color: #f9f9f9;
}
.header-title h1 {
  margin: 0;
  font-size: 15px; /* 18 -> 15 */
  font-weight: bold;
  color: #000;
}
.company-card {
  border: 3px solid #333;
  padding: 16px;
  margin-bottom: 18px;
  background-color: #fafafa;
}
.company-card p {
  margin: 6px 0;
  font-size: 11px; /* ~11pt -> 11px */
  line-height: 1.35;
}
.company-card h2 {
  margin: 0 0 10px 0;
  font-size: 14px; /* ~13pt -> 14px */
  font-weight: bold;
}
.info-card {
  border: 2px solid #333;
  padding: 12px;
  margin-bottom: 18px;
  background-color: #fafafa;
}
.info-card p {
  margin: 5px 0;
  font-size: 11px; /* ~11pt -> 11px */
}
.info-card strong {
  font-weight: bold;
}
.table-container {
  margin: 16px 0;
  overflow-x: auto;
}
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 10px; /* ~10pt -> 10px */
  margin-bottom: 12px;
}
table td, table th {
  border: 0.5pt solid #000;
  padding: 6px; /* réduit pour gagner de l'espace */
  text-align: center;
  vertical-align: top;
}
table th {
  background-color: #f0f0f0;
  font-weight: bold;
}
table td:first-child, table th:first-child {
  text-align: left;
}
.banking-card {
  border: 2px solid #333;
  padding: 12px;
  background-color: #fafafa;
  margin-top: 18px;
}
.banking-card h3 {
  margin: 0 0 10px 0;
  font-size: 11px; /* ~11pt -> 11px */
  font-weight: bold;
}
.banking-card p {
  margin: 5px 0;
  font-size: 11px;
}
.banking-card strong {
  font-weight: bold;
}
.footer {
  margin-top: 24px;
  padding-top: 16px;
  border-top: 1px solid #ddd;
  font-size: 9px; /* ~9pt -> 9px */
  text-align: center;
  color: #666;
}
</style>
</head>
<body>
<div class="facture-container">
<div class="header-title">
<h1>FACTURE EN EUROS</h1>
</div>

<div class="company-card">
<h2>CLAR SERVICES</h2>
<p>SASU au capital de 1000€</p>
<p>37 Avenue Paul Langevin 92260 Fontenay aux Roses</p>
<p>SIRET : 853 533 586 00014</p>
<p>Téléphone : 06 58 83 81 07 / 07 51 02 45 42</p>
<p>Courriel : <span style="color: #0000ff; text-decoration: underline;">infos@clar-services.com</span></p>
</div>

<div class="info-card">
<p><strong>N° :</strong> FAC25-{{NUMERO_FACTURE}}</p>
<p><strong>Date :</strong> {{DATE_FACTURE}}</p>
<p><strong>Date limite de règlement :</strong> Règlement immédiat</p>
<p style="font-size: 12pt; font-weight: bold; margin-top: 10px;">{{NOM_CLIENT}}</p>
<p><strong>Adresse :</strong> {{ADRESSE_CLIENT}}</p>
</div>

<div class="table-container">
<table>
<thead>
<tr>
<th colspan="4" style="text-align: center;">TARIF MENSUEL</th>

</tr>
</thead>
<tbody>
<tr>
<td style="font-weight: bold;">A) SANS PRISE EN CHARGE</td>
<td style="font-weight: bold;">NOMBRE D'HEURES</td>
<td style="font-weight: bold;">TARIF HORAIRE</td>
<td style="font-weight: bold;">TOTAL</td>
</tr>
<tr>
<td>Prestation mensuelle (tous les jours sauf dimanche et férié)</td>
<td>{{HEURES_TOTALES}}</td>
<td><strong>{{TARIF_HORAIRE}}</strong></td>
<td>{{TOTAL_PRESCRIPTION}} €</td>
</tr>
<tr>
<td>Dimanche et jours férié</td>
<td>{{HEURES_FERIE}}</td>
<td><strong>{{TARIF_FERIE}}</strong></td>
<td>{{TOTAL_FERIE}} €</td>
</tr>
<tr>
<td>Total sans prise en charge</td>
<td></td>
<td></td>
<td><strong>{{TOTAL_SANS_PRISE_CHARGE}} €</strong></td>
</tr>
<tr>
<td>Participation du département</td>
<td colspan="2">{{TARIF_DEPARTEMENT}} × {{HEURES_TOTAL_ACCORDEES}} (nombres d'heures accordées)</td>
<td><strong>{{TOTAL_DEPARTEMENT}} €</strong></td>
</tr>
</table>

<table style="width: 100%;">
<tr>
<td style="text-align: left;">Reste mensuel à charge du client *(pour des prestations réalisées tous les jours sauf dimanche et férié)</td>
<td style="text-align: center;"><strong>{{RESTE_A_CHARGE}} €</strong></td>
</tr>
</table>
</div>

<div class="banking-card">
<h3>Coordonnées bancaires</h3>
<p>IBAN : FR76 128 790 000 111 212 803 001 34</p>
<p>BIC (SWIFT) : DELUFR22XXX / Domiciliation : DELUBAC</p>
<p><strong>Total TTC :</strong> {{RESTE_A_CHARGE}} €</p>
<p><strong>Acompte versé :</strong> 0,00 €</p>
<p><strong>T.T.C restant dû :</strong> {{RESTE_A_CHARGE}} €</p>
</div>

<div class="footer">
<p><strong>Pénalités en cas de retard de paiement</strong></p>
<p>En cas de retard de paiement, des pénalités de paiement égales à trois fois le taux d'intérêt légal applicable en France (Art. L441-6 al 3 du code du commerce), et majorées, pour les professionnels, conformément au décret n°2012-1115 du 2 octobre 2012 issu de la loi 2012-387 du 22 mars 2012 d'une indemnité forfaitaire de 40€ (quarante euros) pour frais de recouvrement sont dues, et ce, sans préjudice de toutes indemnités que CLAR SERVICES pourrait réclamer.</p>
</div>
</div>
</body>
</html>
"""

# ==============================
# 🎯 GÉNÉRATION DES FACTURES HTML
# ==============================

def generer_facture_html(row, index):
    """Génère une facture HTML pour un client"""
    
    nom = row.get('noms', '')
    if not nom or pd.isna(nom) or str(nom).lower() == 'nan':
        return None
    
    print(f"📄 Génération facture pour : {nom}")
    
    # Récupération des valeurs de base
    heures_totales = safe_float(
      row.get('heures_semaine', row.get('heures_(semaine)', row.get('heures_totales', 0)))
    )
    heures_ferie = safe_float(
      row.get('heures_dimanches_et_feries', row.get('heures_(dimanches_et_fériés)', row.get('heures_ferie', 0)))
    )
    
    # Tarifs
    tarif_horaire = safe_float(row.get('tarif_horaire_semaine_€/h', 24.58))
    tarif_ferie = safe_float(row.get('tarif_horaire_ferie_€/h', 28.27))
    
    # Calculs des totaux
    total_prescription = round(heures_totales * tarif_horaire, 2)
    total_ferie = round(heures_ferie * tarif_ferie, 2)
    total_sans_prise_charge = round(total_prescription + total_ferie, 2)
    
    # Participation du département
    tarif_departement = safe_float(row.get('prise_en_charge_departement_€/h', 18.18))
    heures_total_accordees = heures_totales + heures_ferie
    total_departement = round(tarif_departement * heures_total_accordees, 2)
    
    # Reste à charge
    reste_a_charge = safe_float(row.get('total_a_payer_par_le_client_ttc_€', row.get('total_a_payer_par_le_client__ttc_€', 0)))
    
    # Données de remplacement
    replacements = {
        '{{NOM_CLIENT}}': str(nom),
        '{{ADRESSE_CLIENT}}': str(row.get('adresse_complete', 'Adresse non fournie')),
        '{{NUMERO_FACTURE}}': f"{index+1:03d}",
        '{{DATE_FACTURE}}': datetime.now().strftime("%d/%m/%Y"),
        '{{HEURES_TOTALES}}': format_nombre(heures_totales),
        '{{TARIF_HORAIRE}}': format_nombre(tarif_horaire),
        '{{TOTAL_PRESCRIPTION}}': format_nombre(total_prescription),
        '{{HEURES_FERIE}}': format_nombre(heures_ferie),
        '{{TARIF_FERIE}}': format_nombre(tarif_ferie),
        '{{TOTAL_FERIE}}': format_nombre(total_ferie),
        '{{TOTAL_SANS_PRISE_CHARGE}}': format_nombre(total_sans_prise_charge),
        '{{TARIF_DEPARTEMENT}}': format_nombre(tarif_departement),
        '{{HEURES_TOTAL_ACCORDEES}}': format_nombre(heures_total_accordees),
        '{{TOTAL_DEPARTEMENT}}': format_nombre(total_departement),
        '{{RESTE_A_CHARGE}}': format_nombre(reste_a_charge)
    }
    
    # Nettoyer les valeurs NaN
    for key, value in replacements.items():
        if str(value).lower() == 'nan' or value is None:
            replacements[key] = '0,00'
    
    # Appliquer les remplacements
    html_content = HTML_TEMPLATE
    for placeholder, valeur in replacements.items():
        html_content = html_content.replace(placeholder, str(valeur))
    
    # Sauvegarder le HTML
    nom_clean = re.sub(r'[<>:"/\\|?*]', '_', str(nom))
    nom_fichier_html = f"FACTURE_{nom_clean}.html"
    chemin_html = os.path.join(DOSSIER_HTML, nom_fichier_html)
    
    with open(chemin_html, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"   ✅ HTML généré : {chemin_html}")
    return chemin_html

def safe_float(value, default=0.0):
    """Convertit une valeur en float de manière sécurisée"""
    try:
        if pd.isna(value):
            return default
        # Remplacer les virgules par des points pour la conversion
        if isinstance(value, str):
            value = value.replace(',', '.')
        return float(value)
    except (ValueError, TypeError):
        return default

def format_nombre(value):
    """Formate un nombre avec 2 décimales et virgule comme séparateur décimal"""
    try:
        if pd.isna(value):
            return "0,00"
        # Assurer que c'est un float
        num = float(value)
        # Formater avec 2 décimales et remplacer le point par une virgule
        return f"{num:,.2f}".replace(',', ' ').replace('.', ',').replace(' ', '.')
    except:
        return "0,00"

# ==============================
# 🚀 CONVERSION HTML VERS PDF
# ==============================

def convertir_html_vers_pdf(chemin_html, nom_client):
    """Convertit un fichier HTML en PDF en préservant le style"""
    try:
        # Lire le fichier HTML
        with open(chemin_html, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Générer le PDF
        chemin_pdf = chemin_html.replace('.html', '.pdf').replace(DOSSIER_HTML, DOSSIER_SORTIE)
        
        # Créer le répertoire s'il n'existe pas
        os.makedirs(os.path.dirname(chemin_pdf), exist_ok=True)
        
        # Convertir HTML en PDF avec WeasyPrint
        HTML(string=html_content).write_pdf(chemin_pdf)
        print(f"   ✅ PDF généré : {chemin_pdf}")
        
        # Uploader le PDF vers Cloudinary
        mois_annee = datetime.now().strftime("%Y-%m")
        resultat_pdf = uploader_vers_cloudinary(chemin_pdf, nom_client, mois_annee, "pdf")
        
        if resultat_pdf['success']:
            print(f"   ☁️  URL Cloudinary: {resultat_pdf['url']}")
        
        # Uploader aussi le HTML
        resultat_html = uploader_vers_cloudinary(chemin_html, nom_client, mois_annee, "html")
        
        if resultat_html['success']:
            print(f"   ☁️  HTML URL: {resultat_html['url']}")
        
        return chemin_pdf
        
    except ImportError:
        print("   ⚠️  WeasyPrint non installé, installation...")
        os.system("pip install weasyprint")
        return convertir_html_vers_pdf(chemin_html, nom_client)
    except Exception as e:
        print(f"   ❌ Erreur conversion PDF: {e}")
        return None

# ==============================
# 🎯 PROGRAMME PRINCIPAL
# ==============================

def main():
    print("🚀 DÉMARRAGE GÉNÉRATION FACTURES")
    print("=" * 50)
    
    start_time = datetime.now()
    tracker.start_time = start_time
    tracker.update(1, 5, "starting", "Initialisation en cours")
    
    # Initialiser Cloudinary
    initialiser_cloudinary()
    
    # Charger les données
    tracker.update(3, 5, "loading", "Chargement des données Excel")
    df = charger_donnees()
    
    if len(df) == 0:
        print("❌ Aucune donnée à traiter")
        tracker.set_error("Aucune donnée à traiter")
        return
    
    total_clients = len(df)
    tracker.total_steps = total_clients + 5
    
    print(f"\n🎯 Génération des factures pour {total_clients} clients...")
    tracker.update(4, total_clients + 5, "processing", f"Génération des factures pour {total_clients} clients")
    
    factures_generees = 0
    mois_annee = datetime.now().strftime("%Y-%m")
    
    for index, row in df.iterrows():
        try:
            # Récupérer le nom du client
            nom_client = row.get('noms', '')
            if not nom_client or pd.isna(nom_client) or str(nom_client).lower() == 'nan':
                continue
            
            # Mettre à jour la progression
            current_step = 4 + index + 1
            tracker.update(current_step, total_clients + 5, "processing", f"Génération facture: {nom_client}")
            
            # Générer le HTML
            chemin_html = generer_facture_html(row, index)
            
            if chemin_html:
                # Convertir en PDF et uploader vers Cloudinary
                chemin_pdf = convertir_html_vers_pdf(chemin_html, nom_client)
                if chemin_pdf:
                    factures_generees += 1
                    
        except Exception as e:
            print(f"❌ Erreur ligne {index}: {e}")
            tracker.set_error(f"Erreur ligne {index}: {e}")
    
    # Finalisation
    end_time = datetime.now()
    duree = str(end_time - start_time).split('.')[0]
    tracker.end_time = end_time
    
    print(f"\n📊 RÉCAPITULATIF:")
    print(f"✅ Factures générées: {factures_generees}/{total_clients}")
    print(f"📁 Dossier HTML: {os.path.abspath(DOSSIER_HTML)}")
    print(f"📁 Dossier PDF: {os.path.abspath(DOSSIER_SORTIE)}")
    print(f"⏱️  Durée: {duree}")
    print("🎉 Terminé !")
    
    # Envoyer le résumé
    tracker.update(total_clients + 5, total_clients + 5, "completed", "Génération terminée")
    send_summary(total_clients, factures_generees, duree, mois_annee)

if __name__ == "__main__":
    main()
