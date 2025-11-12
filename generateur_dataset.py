"""
Générateur de datasets sources pour la clinique avec problèmes de qualité
Simule 3 systèmes sources : Rendez-vous, Laboratoire, Facturation
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('fr_FR')
random.seed(42)

# ============================================================
# SYSTÈME 1 : GESTION DES RENDEZ-VOUS
# ============================================================
print("Génération : Système Rendez-vous...")

rendez_vous_patients = []
medecins_rdv = []

# Liste de médecins avec variations de noms (problème qualité)
medecins_base = [
    {"nom": "Dubois", "prenom": "Marie", "specialite": "Cardiologie"},
    {"nom": "Martin", "prenom": "Jean", "specialite": "Pédiatrie"},
    {"nom": "Bernard", "prenom": "Sophie", "specialite": "Radiologie"},
    {"nom": "Petit", "prenom": "Pierre", "specialite": "Chirurgie"},
    {"nom": "Leroy", "prenom": "Émilie", "specialite": "Gynécologie"},
]

# Générer 100 patients avec des doublons intentionnels
patients_uniques = []
for i in range(80):
    patients_uniques.append({
        'patient_id_rdv': f'RDV-{i+1:04d}',
        'nom': fake.last_name().upper(),
        'prenom': fake.first_name(),
        'date_naissance': fake.date_of_birth(minimum_age=1, maximum_age=90).strftime('%d/%m/%Y'),
        'telephone': fake.phone_number(),
        'email': fake.email(),
        'adresse': fake.address().replace('\n', ', ')
    })

# Ajouter 20 doublons avec variations (nom mal orthographié, email différent, etc.)
doublons = random.sample(patients_uniques, 20)
for patient in doublons:
    doublon = patient.copy()
    doublon['patient_id_rdv'] = f'RDV-{len(patients_uniques)+1:04d}'
    # Variations intentionnelles
    if random.random() > 0.5:
        doublon['nom'] = doublon['nom'].replace('E', 'É')  # Variation accent
    if random.random() > 0.5:
        doublon['telephone'] = fake.phone_number()  # Téléphone différent
    if random.random() > 0.3:
        doublon['email'] = fake.email()  # Email différent
    patients_uniques.append(doublon)

# Créer des rendez-vous
for patient in patients_uniques:
    nb_rdv = random.randint(1, 5)
    for _ in range(nb_rdv):
        medecin = random.choice(medecins_base)
        date_rdv = datetime.now() - timedelta(days=random.randint(0, 365))
        rendez_vous_patients.append({
            'rdv_id': f'RDV-{len(rendez_vous_patients)+1:06d}',
            'patient_id': patient['patient_id_rdv'],
            'patient_nom': patient['nom'],
            'patient_prenom': patient['prenom'],
            'medecin_nom': medecin['nom'],
            'medecin_prenom': medecin['prenom'],
            'specialite': medecin['specialite'],
            'date_rdv': date_rdv.strftime('%Y-%m-%d'),
            'heure_rdv': f"{random.randint(8,18):02d}:{random.choice(['00','15','30','45'])}",
            'statut': random.choice(['Confirmé', 'Annulé', 'Terminé', 'En attente'])
        })

# Médecins du système RDV (avec variations de format)
for med in medecins_base:
    medecins_rdv.append({
        'medecin_id_rdv': f'MED-RDV-{len(medecins_rdv)+1:03d}',
        'nom_complet': f"Dr. {med['prenom']} {med['nom']}",  # Format différent
        'specialite': med['specialite'],
        'telephone_cabinet': fake.phone_number(),
        'email_pro': f"{med['prenom'].lower()}.{med['nom'].lower()}@clinique-rdv.fr"
    })

df_patients_rdv = pd.DataFrame(patients_uniques)
df_rdv = pd.DataFrame(rendez_vous_patients)
df_medecins_rdv = pd.DataFrame(medecins_rdv)

# ============================================================
# SYSTÈME 2 : LABORATOIRE
# ============================================================
print("Génération : Système Laboratoire...")

labo_patients = []
labo_analyses = []

# Réutiliser certains patients avec d'autres IDs (problème!)
for i, patient in enumerate(random.sample(patients_uniques, 60)):
    # ID différent du système RDV!
    labo_patient = {
        'patient_id_labo': f'LAB-P-{i+1:05d}',
        'nom_patient': patient['nom'],
        'prenom_patient': patient['prenom'],
        'ddn': patient['date_naissance'],  # Format différent intentionnel
        'sexe': random.choice(['M', 'F', 'H', 'Homme', 'Femme']),  # Formats incohérents
        'numero_secu': f"{random.randint(1,2)}{fake.random_number(digits=14)}",
    }
    labo_patients.append(labo_patient)
    
    # Analyses pour ce patient
    nb_analyses = random.randint(1, 8)
    for _ in range(nb_analyses):
        date_analyse = datetime.now() - timedelta(days=random.randint(0, 730))
        labo_analyses.append({
            'analyse_id': f'ANA-{len(labo_analyses)+1:07d}',
            'patient_id': labo_patient['patient_id_labo'],
            'type_analyse': random.choice([
                'Numération Formule Sanguine', 'Glycémie', 'Cholestérol', 
                'TSH', 'Créatinine', 'Urée', 'Transaminases', 'CRP'
            ]),
            'date_prelevement': date_analyse.strftime('%d-%m-%Y'),  # Format différent!
            'date_resultat': (date_analyse + timedelta(days=random.randint(1,5))).strftime('%d-%m-%Y'),
            'prescripteur': random.choice([m['nom'] for m in medecins_base]),  # Juste nom!
            'service_labo': random.choice(['Biochimie', 'Hématologie', 'Microbiologie']),
            'urgent': random.choice(['Oui', 'Non', 'OUI', 'NON', '1', '0'])  # Incohérence!
        })

df_patients_labo = pd.DataFrame(labo_patients)
df_analyses = pd.DataFrame(labo_analyses)

# Services laboratoire
services_labo = [
    {'service_id': 'SRV-LAB-001', 'nom_service': 'Biochimie', 'chef_service': 'Dr. Dubois', 'extension': '4501'},
    {'service_id': 'SRV-LAB-002', 'nom_service': 'Hématologie', 'chef_service': 'Pr. Martin', 'extension': '4502'},
    {'service_id': 'SRV-LAB-003', 'nom_service': 'Microbiologie', 'chef_service': 'Dr. Bernard', 'extension': '4503'},
]
df_services_labo = pd.DataFrame(services_labo)

# ============================================================
# SYSTÈME 3 : FACTURATION
# ============================================================
print("Génération : Système Facturation...")

facturation_patients = []
factures = []

# Encore d'autres IDs pour les mêmes patients!
for i, patient in enumerate(random.sample(patients_uniques, 70)):
    fact_patient = {
        'id_patient_fact': f'FACT-{i+1:06d}',
        'nom_famille': patient['nom'],
        'prenoms': patient['prenom'],  # Colonne nommée différemment
        'date_naiss': patient['date_naissance'],
        'tel': patient['telephone'][:10],  # Seulement 10 chiffres
        'mail': patient.get('email', ''),
        'mutuelle': random.choice(['MGEN', 'Harmonie', 'MAAF', 'AXA', 'Generali', 'Aucune', None, '']),  # Nulls!
        'num_mutuelle': f"{random.randint(1000000,9999999)}" if random.random() > 0.3 else None
    }
    facturation_patients.append(fact_patient)
    
    # Factures
    nb_factures = random.randint(1, 10)
    for _ in range(nb_factures):
        date_fact = datetime.now() - timedelta(days=random.randint(0, 500))
        medecin = random.choice(medecins_base)
        factures.append({
            'facture_id': f'F-{len(factures)+1:08d}',
            'patient_id': fact_patient['id_patient_fact'],
            'date_facture': date_fact.strftime('%Y/%m/%d'),  # Encore un format différent!
            'medecin_facturation': f"{medecin['prenom'][0]}. {medecin['nom']}",  # Format abrégé
            'service_facture': random.choice(['Consultation', 'Analyse', 'Imagerie', 'Chirurgie', 'Urgences']),
            'montant_total': round(random.uniform(25, 1500), 2),
            'montant_rembourse': lambda mt: round(mt * random.uniform(0.5, 0.9), 2),
            'statut_paiement': random.choice(['Payé', 'En attente', 'Remboursé', 'Impayé', 'PAYE', 'Payée'])  # Incohérence
        })

# Calculer montant remboursé
for facture in factures:
    facture['montant_rembourse'] = round(facture['montant_total'] * random.uniform(0.5, 0.9), 2)

df_patients_fact = pd.DataFrame(facturation_patients)
df_factures = pd.DataFrame(factures)

# Services facturation
services_fact = [
    {'code_service': 'CONS', 'libelle': 'Consultation', 'tarif_base': 25.0},
    {'code_service': 'ANA', 'libelle': 'Analyse', 'tarif_base': 45.0},
    {'code_service': 'IMG', 'libelle': 'Imagerie', 'tarif_base': 120.0},
    {'code_service': 'CHIR', 'libelle': 'Chirurgie', 'tarif_base': 800.0},
    {'code_service': 'URG', 'libelle': 'Urgences', 'tarif_base': 100.0},
]
df_services_fact = pd.DataFrame(services_fact)

# ============================================================
# SAUVEGARDE DES FICHIERS CSV
# ============================================================
print("\nSauvegarde des fichiers CSV...")

# Système Rendez-vous
df_patients_rdv.to_csv('source_rdv_patients.csv', index=False, encoding='utf-8-sig')
df_rdv.to_csv('source_rdv_consultations.csv', index=False, encoding='utf-8-sig')
df_medecins_rdv.to_csv('source_rdv_medecins.csv', index=False, encoding='utf-8-sig')

# Système Laboratoire
df_patients_labo.to_csv('source_labo_patients.csv', index=False, encoding='utf-8-sig')
df_analyses.to_csv('source_labo_analyses.csv', index=False, encoding='utf-8-sig')
df_services_labo.to_csv('source_labo_services.csv', index=False, encoding='utf-8-sig')

# Système Facturation
df_patients_fact.to_csv('source_fact_patients.csv', index=False, encoding='utf-8-sig')
df_factures.to_csv('source_fact_factures.csv', index=False, encoding='utf-8-sig')
df_services_fact.to_csv('source_fact_services.csv', index=False, encoding='utf-8-sig')

print(f"""
✅ Génération terminée !

FICHIERS CRÉÉS (9 fichiers CSV) :
================================

📁 SYSTÈME RENDEZ-VOUS (3 fichiers)
   - source_rdv_patients.csv ({len(df_patients_rdv)} lignes) - DOUBLONS PRÉSENTS
   - source_rdv_consultations.csv ({len(df_rdv)} lignes)
   - source_rdv_medecins.csv ({len(df_medecins_rdv)} lignes)

📁 SYSTÈME LABORATOIRE (3 fichiers)
   - source_labo_patients.csv ({len(df_patients_labo)} lignes) - IDs DIFFÉRENTS!
   - source_labo_analyses.csv ({len(df_analyses)} lignes)
   - source_labo_services.csv ({len(df_services_labo)} lignes)

📁 SYSTÈME FACTURATION (3 fichiers)
   - source_fact_patients.csv ({len(df_patients_fact)} lignes) - ENCORE D'AUTRES IDs!
   - source_fact_factures.csv ({len(df_factures)} lignes)
   - source_fact_services.csv ({len(df_services_fact)} lignes)

🔍 PROBLÈMES DE QUALITÉ SIMULÉS :
===================================
✗ Doublons patients avec variations (accents, orthographe)
✗ IDs différents pour les mêmes patients dans chaque système
✗ Formats de dates incohérents (JJ/MM/AAAA, AAAA-MM-JJ, DD-MM-YYYY)
✗ Formats de noms médecins différents (Dr. Prénom Nom, P. Nom, Nom)
✗ Valeurs nulles et manquantes (mutuelle, email)
✗ Formats booléens incohérents (Oui/Non, OUI/NON, 1/0)
✗ Casse incohérente (Payé/PAYE/Payée)
✗ Colonnes nommées différemment (nom/nom_famille/nom_patient)

➡️  Ces problèmes démontrent la NÉCESSITÉ du MDM!
""")

print("\n📊 STATISTIQUES DES DONNÉES :")
print(f"Total patients uniques (réalité) : ~80")
print(f"Total enregistrements patients (systèmes) : {len(df_patients_rdv) + len(df_patients_labo) + len(df_patients_fact)}")
print(f"Total rendez-vous : {len(df_rdv)}")
print(f"Total analyses : {len(df_analyses)}")
print(f"Total factures : {len(df_factures)}")