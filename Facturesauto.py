import pandas as pd
from datetime import datetime
import os
import re
import asyncio
import io
import json
import sys
import time
from pyppeteer import launch
import pyppeteer
import cloudinary
import cloudinary.uploader
import cloudinary.api
import requests

pyppeteer.chromium_downloader.download_chromium = lambda *args, **kwargs: None

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
# ⚙️ CONFIG LOCALE TEMPORAIRE
# ==============================
DOSSIER_TEMP = "temp_factures"
DOSSIER_HTML_TEMP = "temp_html"

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
        return True
    except Exception as e:
        tracker.set_error(f"Erreur d'initialisation Cloudinary: {e}")
        return False

# ==============================
# ☁️ FONCTIONS CLOUDINARY
# ==============================
def telecharger_excel_depuis_cloudinary():
    """
    Télécharge le fichier Excel depuis Cloudinary en utilisant l'API de recherche.
    """
    import io
    import requests
    try:
        print(f"📥 Recherche du fichier Excel dans le dossier 'BDD'...")
        
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
        # Ne pas supprimer en cas d'erreur
        return {'success': False, 'error': str(e)}

def lister_contenu_dossier_cloudinary(dossier=""):
    """Liste le contenu d'un dossier Cloudinary"""
    try:
        prefix = f"{dossier}/" if dossier else ""
        
        resultats = cloudinary.api.resources(
            type="upload",
            prefix=prefix,
            max_results=100
        )
        
        if 'resources' in resultats and resultats['resources']:
            print(f"\n📁 Contenu de '{dossier if dossier else 'racine'}':")
            
            # Organiser par type
            fichiers_pdf = []
            fichiers_html = []
            fichiers_excel = []
            autres = []
            
            for res in resultats['resources']:
                fichier_info = {
                    'nom': res['public_id'],
                    'format': res.get('format', 'N/A'),
                    'taille': f"{res.get('bytes', 0)/1024:.1f}KB",
                    'date': res.get('created_at', 'N/A')
                }
                
                if res.get('format') == 'pdf':
                    fichiers_pdf.append(fichier_info)
                elif res.get('format') == 'html' or 'html' in res['public_id'].lower():
                    fichiers_html.append(fichier_info)
                elif res.get('format') in ['xlsx', 'xls']:
                    fichiers_excel.append(fichier_info)
                else:
                    autres.append(fichier_info)
            
            if fichiers_excel:
                print(f"\n📊 Fichiers Excel ({len(fichiers_excel)}):")
                for f in fichiers_excel:
                    print(f"   • {f['nom']} ({f['format']}, {f['taille']})")
            
            if fichiers_pdf:
                print(f"\n📄 Factures PDF ({len(fichiers_pdf)}):")
                for f in fichiers_pdf[:5]:  # Afficher seulement les 5 premiers
                    print(f"   • {f['nom']} ({f['taille']})")
                if len(fichiers_pdf) > 5:
                    print(f"   ... et {len(fichiers_pdf) - 5} autres")
            
            if fichiers_html:
                print(f"\n🌐 Fichiers HTML ({len(fichiers_html)}):")
                for f in fichiers_html[:3]:
                    print(f"   • {f['nom']} ({f['taille']})")
                if len(fichiers_html) > 3:
                    print(f"   ... et {len(fichiers_html) - 3} autres")
            
            return resultats
        else:
            print(f"\n📁 Dossier '{dossier}' vide ou inexistant")
            return None
            
    except Exception as e:
        print(f"❌ Erreur listing Cloudinary: {e}")
        return None

def creer_dossier_cloudinary(dossier_path):
    """Crée un dossier sur Cloudinary (simulation)"""
    try:
        # Cloudinary n'a pas de vraie API pour créer des dossiers
        # On va créer un fichier vide pour "marquer" le dossier
        nom_fichier_temp = "temp_marker.txt"
        chemin_temp = os.path.join(DOSSIER_TEMP, nom_fichier_temp)
        
        with open(chemin_temp, 'w') as f:
            f.write("Dossier créé automatiquement")
        
        resultat = cloudinary.uploader.upload(
            chemin_temp,
            folder=dossier_path,
            public_id=".folder_marker",
            overwrite=False,
            tags=["dossier_marker"]
        )
        
        os.remove(chemin_temp)
        print(f"✅ Dossier créé/marqué: {dossier_path}")
        return True
        
    except Exception as e:
        print(f"⚠️  Impossible de créer le dossier: {e}")
        return False

# ==============================
# 🔍 CHARGEMENT DES DONNÉES DEPUIS CLOUDINARY
# ==============================
def charger_donnees_depuis_cloudinary():
    """Charge et nettoie les données Excel depuis Cloudinary"""
    print("\n🔍 Chargement des données depuis Cloudinary...")
    
    # Télécharger le DataFrame depuis Cloudinary
    df_complet = telecharger_excel_depuis_cloudinary()
    
    if df_complet is None or len(df_complet) == 0:
        print("❌ Impossible de charger les données depuis Cloudinary")
        return pd.DataFrame()
    
    print("🔍 Recherche de la position des données dans le fichier...")
    
    # Recherche de la ligne contenant "Noms"
    ligne_titre = None
    for i in range(min(50, len(df_complet))):  # Chercher dans les 50 premières lignes
        for j in range(min(10, len(df_complet.columns))):  # Et les 10 premières colonnes
            valeur_cellule = str(df_complet.iloc[i, j]).strip()
            if valeur_cellule.lower() == "noms":
                ligne_titre = i
                print(f"✅ 'Noms' trouvé à la position : Ligne {i+1}, Colonne {j+1}")
                break
        if ligne_titre is not None:
            break
    
    if ligne_titre is None:
        print("⚠️  'Noms' non trouvé, tentative de détection automatique...")
        # Essayer de trouver une ligne avec des en-têtes
        for i in range(min(20, len(df_complet))):
            # Vérifier si cette ligne contient plusieurs mots (probables en-têtes)
            nb_mots = sum(1 for j in range(min(10, len(df_complet.columns))) 
                         if isinstance(df_complet.iloc[i, j], str) and len(df_complet.iloc[i, j].split()) > 0)
            if nb_mots >= 3:  # Au moins 3 colonnes avec du texte
                ligne_titre = i
                print(f"📝 Ligne {i+1} détectée comme en-têtes (contient {nb_mots} colonnes avec texte)")
                break
    
    if ligne_titre is None:
        print("❌ Impossible de détecter les en-têtes, utilisation ligne 10 par défaut")
        ligne_titre = 9  # Ligne 10 en index 0-based
    
    # Charger les données à partir de la ligne d'en-têtes
    try:
        df = pd.DataFrame(df_complet.iloc[ligne_titre+1:].values, columns=df_complet.iloc[ligne_titre])
    except:
        print("⚠️  Erreur lors de la création du DataFrame, tentative alternative...")
        df = df_complet.iloc[ligne_titre+1:].copy()
        df.columns = df_complet.iloc[ligne_titre].tolist()
    
    # Nettoyer les colonnes
    colonnes_a_garder = []
    for col in df.columns:
        col_str = str(col)
        if 'unnamed' not in col_str.lower() and not col_str.startswith('Unnamed') and not pd.isna(col):
            colonnes_a_garder.append(col)
    
    if colonnes_a_garder:
        df = df[colonnes_a_garder]
    else:
        print("⚠️  Aucune colonne valide trouvée, utilisation de toutes les colonnes")
    
    print("\n📋 Colonnes originales trouvées :")
    for col in df.columns:
        print(f"  - '{col}'")
    
    # Nettoyer les noms de colonnes
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace('\xa0', ' ', regex=False)
        .str.replace(' ', '_', regex=False)
        .str.replace('é', 'e', regex=False)
        .str.replace('è', 'e', regex=False)
        .str.replace('à', 'a', regex=False)
        .str.replace('(', '', regex=False)
        .str.replace(')', '', regex=False)
        .str.replace('-', '_', regex=False)
    )
    
    print("\n📋 Colonnes après nettoyage :")
    for col in df.columns:
        print(f"  - '{col}'")
    
    # Supprimer les lignes vides
    if 'noms' in df.columns:
        df = df.dropna(subset=['noms'])
        df = df.reset_index(drop=True)
    else:
        print("\n⚠️  Colonne 'noms' non trouvée après nettoyage !")
        print("   Tentative de trouver une colonne similaire...")
        
        colonnes_similaires = [col for col in df.columns if 'nom' in col.lower()]
        if colonnes_similaires:
            print(f"   Colonnes similaires trouvées: {colonnes_similaires}")
            df = df.dropna(subset=[colonnes_similaires[0]])
            df = df.reset_index(drop=True)
            print(f"   Utilisation de '{colonnes_similaires[0]}' comme colonne noms")
        else:
            print("❌ Aucune colonne 'nom' trouvée")
            print("   Colonnes disponibles:", list(df.columns))
            return pd.DataFrame()
    
    print(f"\n📊 {len(df)} clients trouvés")
    
    # Sauvegarder un extrait localement pour debug (optionnel)
    chemin_debug = os.path.join(DOSSIER_TEMP, "debug_data.csv")
    df.head(10).to_csv(chemin_debug, index=False, encoding='utf-8')
    print(f"📝 Extrait sauvegardé pour debug: {chemin_debug}")
    
    return df

# ==============================
# 🎯 TEMPLATE HTML (inchangé)
# ==============================
HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
body {
  margin-left: 5%;
  margin-right: 5%;
  padding: 20px;
  font-family: Arial, sans-serif;
  background-color: #f5f5f5;
}
.facture-container {
  width: 85%;
  margin: 0 auto;
  background-color: white;
    padding: 24px;
  box-shadow: 0 0 10px rgba(0,0,0,0.1);
}
.header-title {
    border: 3px solid #333;
    padding: 10px;
    text-align: center;
    margin-bottom: 12px;
    background-color: #f9f9f9;
}
.header-title h1 {
    margin: 0;
    font-size: 13px;
    font-weight: bold;
    color: #000;
}
.company-card {
    border: 3px solid #333;
    padding: 10px;
    margin-bottom: 12px;
    background-color: #fafafa;
}
.company-card p {
    margin: 4px 0;
    font-size: 8pt;
    line-height: 1.25;
}
.company-card h2 {
    margin: 0 0 8px 0;
    font-size: 10pt;
    font-weight: bold;
}
.info-card {
    border: 2px solid #333;
    padding: 10px;
    margin-bottom: 12px;
    background-color: #fafafa;
}
.info-card p {
    margin: 3px 0;
    font-size: 8pt;
}
.info-card strong {
  font-weight: bold;
}
.table-container {
    margin: 12px 0;
    overflow-x: auto;
}
table {
    width: 100%;
    border-collapse: collapse;
    font-size: 7pt;
    margin-bottom: 10px;
}
table td, table th {
    border: 0.5pt solid #000;
    padding: 4px 2px;
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
    padding: 10px;
    background-color: #fafafa;
    margin-top: 12px;
}
.banking-card h3 {
    margin: 0 0 8px 0;
    font-size: 8pt;
    font-weight: bold;
}
.banking-card p {
    margin: 3px 0;
    font-size: 8pt;
}
.banking-card strong {
  font-weight: bold;
}
.footer {
    margin-top: 10px;
    padding-top: 10px;
    border-top: 1px solid #ddd;
    font-size: 7pt;
    text-align: center;
    color: #666;
    line-height: 1.25;
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
<th colspan="2">TARIF MENSUEL</th>
<th>TARIF HORAIRE</th>
<th>TOTAL</th>
</tr>
</thead>
<tbody>
<tr>
<td style="font-weight: bold;">A) SANS PRISE EN CHARGE</td>
<td style="font-weight: bold;">NOMBRE D'HEURES</td>
<td></td>
<td></td>
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
    
    print(f"\n📄 Génération facture pour : {nom}")
    
    heures_totales = safe_float(
        row.get('heures_semaine',
               row.get('heures_totales', 0))
    )
    
    heures_ferie = safe_float(
        row.get('heures_dimanches_et_feries',
               0)
    )
    
    tarif_horaire = 24.58
    tarif_ferie = 28.27
    
    total_prescription = round(heures_totales * tarif_horaire, 2)
    total_ferie = round(heures_ferie * tarif_ferie, 2)
    total_sans_prise_charge = round(total_prescription + total_ferie, 2)
    
    tarif_departement = 18.39
    for col_name in ['prise_en_charge_departement_e_h', 'prise_en_charge_departement', 'tarif_departement']:
        if col_name in row:
            tarif_departement = safe_float(row[col_name], 18.18)
            break
    
    heures_total_accordees = heures_totales + heures_ferie
    total_departement = safe_float(row.get('total_a_payer_par_le_departement_€', row.get('total_a_payer_par_le_departement_€', 0)))
    reste_a_charge = safe_float(row.get('total_a_payer_par_le_client_ttc_€', row.get('total_a_payer_par_le_client_ttc€', 0)))
    
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
    
    for key, value in replacements.items():
        if str(value).lower() == 'nan' or value is None:
            replacements[key] = '0,00'
    
    html_content = HTML_TEMPLATE
    for placeholder, valeur in replacements.items():
        html_content = html_content.replace(placeholder, str(valeur))
    
    nom_clean = re.sub(r'[<>:"/\\|?*]', '_', str(nom))
    nom_fichier_html = f"FACTURE_{nom_clean}.html"
    chemin_html = os.path.join(DOSSIER_HTML_TEMP, nom_fichier_html)
    
    with open(chemin_html, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ HTML généré temporairement: {chemin_html}")
    return chemin_html, nom

def safe_float(value, default=0.0):
    """Convertit une valeur en float de manière sécurisée"""
    try:
        if pd.isna(value):
            return default
        if isinstance(value, str):
            value = value.replace(',', '.').replace(' ', '').replace('€', '')
        return float(value)
    except (ValueError, TypeError):
        return default

def format_nombre(value):
    """Formate un nombre avec 2 décimales et virgule comme séparateur décimal"""
    try:
        if pd.isna(value):
            return "0,00"
        num = float(value)
        return f"{num:,.2f}".replace(',', ' ').replace('.', ',').replace(' ', '.')
    except:
        return "0,00"

# ==============================
# 🚀 CONVERSION HTML VERS PDF (Pyppeteer)
# ==============================
async def convertir_html_vers_pdf_async(chemin_html, nom_client):
    """Convertit un fichier HTML en PDF avec Pyppeteer"""
    try:
        nom_clean = re.sub(r'[<>:"/\\|?*]', '_', str(nom_client))
        nom_fichier_pdf = f"FACTURE_{nom_clean}_{datetime.now().strftime('%Y%m%d')}.pdf"
        chemin_pdf_temp = os.path.join(DOSSIER_TEMP, nom_fichier_pdf)
        
        os.makedirs(os.path.dirname(chemin_pdf_temp), exist_ok=True)
        
        chrome_paths = [
            "C:/Program Files/Google/Chrome/Application/chrome.exe",
            "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe",
            os.environ.get('LOCALAPPDATA', '') + "/Google/Chrome/Application/chrome.exe",
            os.environ.get('PROGRAMFILES', '') + "/Google/Chrome/Application/chrome.exe",
            os.environ.get('PROGRAMFILES(X86)', '') + "/Google/Chrome/Application/chrome.exe"
        ]
        
        chrome_executable = None
        for path in chrome_paths:
            if os.path.exists(path):
                chrome_executable = path
                break
        
        if chrome_executable:
            browser = await launch(
                executablePath=chrome_executable, 
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
        else:
            browser = await launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
        
        page = await browser.newPage()
        await page.setViewport({'width': 1240, 'height': 1754})
        
        html_path = os.path.abspath(chemin_html)
        await page.goto(f'file:///{html_path}', waitUntil='networkidle2')
        
        pdf_options = {
            'path': chemin_pdf_temp,
            'format': 'A4',
            'printBackground': True,
            'margin': {
                'top': '20mm',
                'right': '15mm',
                'bottom': '20mm',
                'left': '15mm'
            },
            'preferCSSPageSize': True
        }
        
        await page.pdf(pdf_options)
        await browser.close()
        
        print(f"   ✅ PDF généré temporairement: {nom_fichier_pdf}")
        return chemin_pdf_temp
        
    except Exception as e:
        print(f"   ❌ Erreur conversion PDF: {e}")
        import traceback
        traceback.print_exc()
        return None

def convertir_html_vers_pdf(chemin_html, nom_client):
    """Wrapper synchrone pour la conversion PDF asynchrone"""
    return asyncio.run(convertir_html_vers_pdf_async(chemin_html, nom_client))

# ==============================
# 🧹 NETTOYAGE DES FICHIERS TEMPORAIRES
# ==============================
def nettoyer_fichiers_temporaires():
    """Supprime tous les fichiers temporaires"""
    import shutil
    try:
        if os.path.exists(DOSSIER_TEMP):
            shutil.rmtree(DOSSIER_TEMP)
            os.makedirs(DOSSIER_TEMP, exist_ok=True)
            print("✅ Dossier temporaire des PDF nettoyé")
        
        if os.path.exists(DOSSIER_HTML_TEMP):
            shutil.rmtree(DOSSIER_HTML_TEMP)
            os.makedirs(DOSSIER_HTML_TEMP, exist_ok=True)
            print("✅ Dossier temporaire des HTML nettoyé")
            
    except Exception as e:
        print(f"⚠️  Erreur lors du nettoyage: {e}")

# ==============================
# 🎯 PROGRAMME PRINCIPAL
# ==============================
def main():
    send_progress(0, 100, "starting", "Initialisation du système...", 0)
    
    try:
        # Initialiser Cloudinary
        send_progress(5, 100, "loading", "Connexion à Cloudinary...", 5)
        if not initialiser_cloudinary():
            send_progress(0, 100, "error", "Échec de connexion à Cloudinary", 0, error="Cloudinary error")
            return
        
        # Nettoyer les anciens fichiers
        send_progress(10, 100, "loading", "Nettoyage des fichiers temporaires...", 10)
        nettoyer_fichiers_temporaires()
        
        # Charger les données
        send_progress(15, 100, "loading", "Chargement des données depuis Cloudinary...", 15)
        df = charger_donnees_depuis_cloudinary()
        
        if len(df) == 0:
            send_progress(0, 100, "error", "Aucune donnée à traiter", 0, error="No data")
            return
        
        total_clients = len(df)
        send_progress(20, 100, "processing", f"Début du traitement de {total_clients} clients", 20)
        
        mois_annee = datetime.now().strftime("%Y-%m")
        factures_generees = 0
        urls_result = []
        start_time = datetime.now()
        
        # Traiter chaque client
        for index, row in df.iterrows():
            client_num = index + 1
            # Progression linéaire: 20% -> 95% pour le traitement des clients
            progress = 20 + int((client_num / total_clients) * 75)
            
            nom_client = str(row.get('noms', f'Client {client_num}')).strip()
            send_progress(
                client_num, 
                total_clients, 
                "generating", 
                f"Génération pour: {nom_client}", 
                progress
            )
            
            try:
                # Générer le HTML
                resultat_html = generer_facture_html(row, index)
                if not resultat_html:
                    continue
                    
                chemin_html, nom_client = resultat_html
                
                # Convertir en PDF
                chemin_pdf_temp = convertir_html_vers_pdf(chemin_html, nom_client)
                if not chemin_pdf_temp:
                    continue
                
                # Upload du PDF
                resultat_pdf = uploader_vers_cloudinary(chemin_pdf_temp, nom_client, mois_annee)
                if resultat_pdf.get('success'):
                    factures_generees += 1
                    urls_result.append({
                        'client': nom_client,
                        'url': resultat_pdf.get('url', '#'),
                        'date': datetime.now().isoformat()
                    })
                    
                    # Envoyer la mise à jour avec les URLs
                    send_progress(
                        client_num,
                        total_clients,
                        "uploading",
                        f"✅ {nom_client} - Facture générée",
                        progress,
                        urls=urls_result
                    )
                
                # Upload du HTML (optionnel)
                uploader_vers_cloudinary(chemin_html, nom_client, mois_annee, "html")
                
            except Exception as e:
                send_progress(
                    client_num,
                    total_clients,
                    "error",
                    f"Erreur pour {nom_client}: {str(e)}",
                    progress,
                    urls=urls_result,
                    error=str(e)
                )
                continue
        
        # Finalisation
        end_time = datetime.now()
        duree = f"{(end_time - start_time).total_seconds():.1f}s"
        
        send_progress(
            total_clients,
            total_clients,
            "completed",
            f"✅ Génération terminée! {factures_generees} factures créées",
            100,
            urls=urls_result
        )
        
        send_summary(total_clients, factures_generees, duree, mois_annee)
        
    except Exception as e:
        send_progress(0, 100, "error", f"Erreur fatale: {str(e)}", 0, error=str(e))

# ==============================
# 🔧 FONCTIONS UTILITAIRES SUPPLEMENTAIRES
# ==============================
def tester_connexion_cloudinary():
    """Teste la connexion à Cloudinary"""
    print("🔧 Test de connexion Cloudinary...")
    
    if not all([CLOUDINARY_CONFIG["cloud_name"], 
                CLOUDINARY_CONFIG["api_key"], 
                CLOUDINARY_CONFIG["api_secret"]]):
        print("❌ Configuration Cloudinary incomplète")
        print("Veuillez remplir les informations suivantes:")
        print(f"   Cloud Name: {CLOUDINARY_CONFIG['cloud_name']}")
        print(f"   API Key: {CLOUDINARY_CONFIG['api_key'][:10]}...")
        print(f"   API Secret: {'*' * len(CLOUDINARY_CONFIG['api_secret']) if CLOUDINARY_CONFIG['api_secret'] else 'Non défini'}")
        return False
    
    try:
        initialiser_cloudinary()
        
        # Tester en listant les ressources
        resultats = cloudinary.api.resources(
            type="upload",
            max_results=1
        )
        print("✅ Connexion Cloudinary réussie")
        return True
        
    except Exception as e:
        print(f"❌ Erreur de connexion Cloudinary: {e}")
        return False

def preparer_structure_cloudinary():
    """Prépare la structure de dossiers sur Cloudinary"""
    print("\n🔧 Préparation de la structure Cloudinary...")
    
    # Créer les dossiers principaux
    creer_dossier_cloudinary(CLOUDINARY_BDD_FOLDER)
    creer_dossier_cloudinary(CLOUDINARY_BASE_FOLDER)
    
    print("\n📁 Structure Cloudinary prête:")
    print(f"   • {CLOUDINARY_BDD_FOLDER}/ - Pour vos fichiers Excel")
    print(f"   • {CLOUDINARY_BASE_FOLDER}/ - Pour les factures générées")
    print(f"\n💡 Astuce: Uploader votre donnees.xlsx dans le dossier {CLOUDINARY_BDD_FOLDER}/")

if __name__ == "__main__":
    main()