-- ============================================================
-- CRÉATION DE LA BASE DE DONNÉES MDM CLINIQUE
-- ============================================================
-- Connexion: psql -U mdm_user -d mdm_clinique

-- Création du schéma MDM
CREATE SCHEMA IF NOT EXISTS mdm;

-- ============================================================
-- TABLE 1: MDM_PATIENT
-- ============================================================
-- Description: Référentiel unique des patients de la clinique
-- Sources: Système RDV, Système Laboratoire, Système Facturation
-- Consommateurs: Dossier Patient, Dashboards, Analytics, Alertes

DROP TABLE IF EXISTS mdm.mdm_patient CASCADE;

CREATE TABLE mdm.mdm_patient (
    -- Clé primaire MDM
    patient_id_mdm UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identifiants sources (traçabilité)
    patient_id_rdv VARCHAR(50),
    patient_id_labo VARCHAR(50),
    patient_id_fact VARCHAR(50),
    
    -- Données démographiques (Golden Record)
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    nom_naissance VARCHAR(100), -- Nom de jeune fille
    
    -- Date de naissance normalisée
    date_naissance DATE NOT NULL,
    
    -- Données contact
    sexe CHAR(1) CHECK (sexe IN ('M', 'F')),
    telephone_principal VARCHAR(20),
    telephone_secondaire VARCHAR(20),
    email VARCHAR(255),
    
    -- Adresse
    adresse_ligne1 VARCHAR(255),
    adresse_ligne2 VARCHAR(255),
    code_postal VARCHAR(10),
    ville VARCHAR(100),
    pays VARCHAR(100) DEFAULT 'France',
    
    -- Sécurité sociale
    numero_secu VARCHAR(15) UNIQUE,
    
    -- Mutuelle
    mutuelle_nom VARCHAR(100),
    mutuelle_numero VARCHAR(50),
    
    -- Métadonnées MDM
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_derniere_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_principale VARCHAR(50), -- 'RDV', 'LABO', 'FACT'
    score_qualite DECIMAL(5,2) DEFAULT 0, -- 0-100
    statut_mdm VARCHAR(20) DEFAULT 'Actif' CHECK (statut_mdm IN ('Actif', 'Inactif', 'Doublon', 'À_vérifier')),
    
    -- Audit
    cree_par VARCHAR(100) DEFAULT 'SYSTEM',
    modifie_par VARCHAR(100) DEFAULT 'SYSTEM'
);

-- Index pour améliorer les performances
CREATE INDEX idx_patient_nom_prenom ON mdm.mdm_patient(nom, prenom);
CREATE INDEX idx_patient_date_naissance ON mdm.mdm_patient(date_naissance);
CREATE INDEX idx_patient_numero_secu ON mdm.mdm_patient(numero_secu);
CREATE INDEX idx_patient_source_rdv ON mdm.mdm_patient(patient_id_rdv);
CREATE INDEX idx_patient_source_labo ON mdm.mdm_patient(patient_id_labo);
CREATE INDEX idx_patient_source_fact ON mdm.mdm_patient(patient_id_fact);

COMMENT ON TABLE mdm.mdm_patient IS 'Référentiel Maître des Patients - Vue unique et dédupliquée';
COMMENT ON COLUMN mdm.mdm_patient.score_qualite IS 'Score de qualité calculé sur complétude, exactitude, fraîcheur (0-100)';
COMMENT ON COLUMN mdm.mdm_patient.source_principale IS 'Système source considéré comme référence pour ce patient';


-- ============================================================
-- TABLE 2: MDM_MEDECIN
-- ============================================================
-- Description: Référentiel unique des médecins et praticiens
-- Sources: Système RDV, Système Laboratoire (prescripteurs), Système Facturation
-- Consommateurs: Planning, Facturation, Analyses statistiques, Dossiers patients

DROP TABLE IF EXISTS mdm.mdm_medecin CASCADE;

CREATE TABLE mdm.mdm_medecin (
    -- Clé primaire MDM
    medecin_id_mdm UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identifiants sources
    medecin_id_rdv VARCHAR(50),
    medecin_id_autre VARCHAR(50),
    
    -- Identité
    civilite VARCHAR(10) CHECK (civilite IN ('Dr', 'Pr', 'M', 'Mme')),
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    
    -- Informations professionnelles
    specialite_principale VARCHAR(100) NOT NULL,
    specialites_secondaires TEXT[], -- Array PostgreSQL
    numero_rpps VARCHAR(11) UNIQUE, -- Répertoire Partagé des Professionnels de Santé
    numero_ordre VARCHAR(20),
    
    -- Contact professionnel
    telephone_cabinet VARCHAR(20),
    email_pro VARCHAR(255),
    
    -- Disponibilité
    statut VARCHAR(20) DEFAULT 'Actif' CHECK (statut IN ('Actif', 'Inactif', 'Congé', 'Retraité')),
    date_debut_exercice DATE,
    date_fin_exercice DATE,
    
    -- Services rattachés (relations)
    services_rattaches UUID[], -- Array d'UUIDs vers mdm_service
    
    -- Métadonnées MDM
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_derniere_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    score_qualite DECIMAL(5,2) DEFAULT 0,
    
    -- Audit
    cree_par VARCHAR(100) DEFAULT 'SYSTEM',
    modifie_par VARCHAR(100) DEFAULT 'SYSTEM'
);

CREATE INDEX idx_medecin_nom ON mdm.mdm_medecin(nom, prenom);
CREATE INDEX idx_medecin_specialite ON mdm.mdm_medecin(specialite_principale);
CREATE INDEX idx_medecin_rpps ON mdm.mdm_medecin(numero_rpps);
CREATE INDEX idx_medecin_statut ON mdm.mdm_medecin(statut);

COMMENT ON TABLE mdm.mdm_medecin IS 'Référentiel Maître des Médecins et Praticiens';
COMMENT ON COLUMN mdm.mdm_medecin.numero_rpps IS 'Identifiant national unique du praticien';


-- ============================================================
-- TABLE 3: MDM_SERVICE
-- ============================================================
-- Description: Référentiel unique des services et départements
-- Sources: Système Laboratoire, Système Facturation, Organisation clinique
-- Consommateurs: Facturation, Analytics, Planification, Dashboards direction

DROP TABLE IF EXISTS mdm.mdm_service CASCADE;

CREATE TABLE mdm.mdm_service (
    -- Clé primaire MDM
    service_id_mdm UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Identifiants sources
    service_id_labo VARCHAR(50),
    code_service_fact VARCHAR(50),
    
    -- Identification
    code_service VARCHAR(20) UNIQUE NOT NULL,
    nom_service VARCHAR(100) NOT NULL,
    type_service VARCHAR(50) NOT NULL CHECK (type_service IN (
        'Consultation',
        'Laboratoire',
        'Imagerie',
        'Chirurgie',
        'Urgences',
        'Hospitalisation',
        'Administratif'
    )),
    
    -- Hiérarchie
    service_parent_id UUID REFERENCES mdm.mdm_service(service_id_mdm),
    niveau_hierarchie INT DEFAULT 1,
    
    -- Responsables
    chef_service_id UUID REFERENCES mdm.mdm_medecin(medecin_id_mdm),
    responsable_administratif VARCHAR(100),
    
    -- Contact
    telephone_service VARCHAR(20),
    email_service VARCHAR(255),
    extension_interne VARCHAR(10),
    
    -- Localisation
    batiment VARCHAR(50),
    etage VARCHAR(10),
    numero_salle VARCHAR(20),
    
    -- Tarification
    tarif_base_consultation DECIMAL(10,2),
    tarif_conventionnel BOOLEAN DEFAULT TRUE,
    
    -- Capacités
    nb_lits INT,
    nb_praticiens INT,
    
    -- Disponibilité
    actif BOOLEAN DEFAULT TRUE,
    horaires_ouverture JSONB, -- Format JSON pour flexibilité
    
    -- Métadonnées MDM
    date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_derniere_maj TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    score_qualite DECIMAL(5,2) DEFAULT 0,
    
    -- Audit
    cree_par VARCHAR(100) DEFAULT 'SYSTEM',
    modifie_par VARCHAR(100) DEFAULT 'SYSTEM'
);

CREATE INDEX idx_service_code ON mdm.mdm_service(code_service);
CREATE INDEX idx_service_type ON mdm.mdm_service(type_service);
CREATE INDEX idx_service_actif ON mdm.mdm_service(actif);
CREATE INDEX idx_service_parent ON mdm.mdm_service(service_parent_id);

COMMENT ON TABLE mdm.mdm_service IS 'Référentiel Maître des Services et Départements';
COMMENT ON COLUMN mdm.mdm_service.horaires_ouverture IS 'Format JSON: {"lundi": "08:00-18:00", ...}';


-- ============================================================
-- TABLES DE MAPPING (Traçabilité des correspondances)
-- ============================================================

-- Mapping Patient (pour tracer tous les identifiants sources)
DROP TABLE IF EXISTS mdm.patient_source_mapping CASCADE;

CREATE TABLE mdm.patient_source_mapping (
    mapping_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id_mdm UUID NOT NULL REFERENCES mdm.mdm_patient(patient_id_mdm),
    systeme_source VARCHAR(50) NOT NULL, -- 'RDV', 'LABO', 'FACT'
    id_source VARCHAR(100) NOT NULL,
    date_mapping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    confiance_matching DECIMAL(5,2), -- Score 0-100
    UNIQUE(systeme_source, id_source)
);

COMMENT ON TABLE mdm.patient_source_mapping IS 'Table de correspondance entre IDs MDM et IDs sources';


-- ============================================================
-- TABLES DE QUALITÉ DES DONNÉES
-- ============================================================

-- Historique des scores de qualité
DROP TABLE IF EXISTS mdm.qualite_historique CASCADE;

CREATE TABLE mdm.qualite_historique (
    historique_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_mdm VARCHAR(50) NOT NULL, -- 'MDM_PATIENT', 'MDM_MEDECIN', 'MDM_SERVICE'
    enregistrement_id UUID NOT NULL,
    date_mesure TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Dimensions de qualité (selon le Data Catalogue)
    completude DECIMAL(5,2), -- % de champs remplis
    exactitude DECIMAL(5,2), -- % de données correctes
    coherence DECIMAL(5,2), -- % de règles métier respectées
    fraicheur DECIMAL(5,2), -- Score basé sur date dernière maj
    unicite DECIMAL(5,2), -- Score basé sur détection doublons
    
    score_global DECIMAL(5,2), -- Moyenne pondérée
    commentaire TEXT
);

CREATE INDEX idx_qualite_table ON mdm.qualite_historique(table_mdm);
CREATE INDEX idx_qualite_date ON mdm.qualite_historique(date_mesure);


-- ============================================================
-- VUE: Vue 360° Patient
-- ============================================================

CREATE OR REPLACE VIEW mdm.v_patient_360 AS
SELECT 
    p.patient_id_mdm,
    p.nom,
    p.prenom,
    p.date_naissance,
    p.sexe,
    p.telephone_principal,
    p.email,
    p.numero_secu,
    p.mutuelle_nom,
    p.score_qualite,
    p.statut_mdm,
    
    -- Nombre de sources connectées
    (CASE WHEN p.patient_id_rdv IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN p.patient_id_labo IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN p.patient_id_fact IS NOT NULL THEN 1 ELSE 0 END) AS nb_sources_connectees,
    
    -- Dernière activité
    p.date_derniere_maj,
    
    -- Indicateurs qualité
    CASE 
        WHEN p.score_qualite >= 90 THEN 'Excellent'
        WHEN p.score_qualite >= 70 THEN 'Bon'
        WHEN p.score_qualite >= 50 THEN 'Moyen'
        ELSE 'Faible'
    END AS niveau_qualite
    
FROM mdm.mdm_patient p;

COMMENT ON VIEW mdm.v_patient_360 IS 'Vue enrichie du patient avec indicateurs de qualité';


-- ============================================================
-- FONCTION: Calcul du score de qualité
-- ============================================================

CREATE OR REPLACE FUNCTION mdm.calculer_score_qualite_patient(p_patient_id UUID)
RETURNS DECIMAL(5,2) AS $$
DECLARE
    v_score DECIMAL(5,2) := 0;
    v_completude DECIMAL(5,2) := 0;
    v_nb_champs_total INT := 15; -- Nombre de champs importants
    v_nb_champs_remplis INT := 0;
BEGIN
    -- Calcul de la complétude
    SELECT 
        (CASE WHEN nom IS NOT NULL AND nom <> '' THEN 1 ELSE 0 END +
         CASE WHEN prenom IS NOT NULL AND prenom <> '' THEN 1 ELSE 0 END +
         CASE WHEN date_naissance IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN sexe IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN telephone_principal IS NOT NULL AND telephone_principal <> '' THEN 1 ELSE 0 END +
         CASE WHEN email IS NOT NULL AND email <> '' THEN 1 ELSE 0 END +
         CASE WHEN adresse_ligne1 IS NOT NULL AND adresse_ligne1 <> '' THEN 1 ELSE 0 END +
         CASE WHEN code_postal IS NOT NULL AND code_postal <> '' THEN 1 ELSE 0 END +
         CASE WHEN ville IS NOT NULL AND ville <> '' THEN 1 ELSE 0 END +
         CASE WHEN numero_secu IS NOT NULL AND numero_secu <> '' THEN 1 ELSE 0 END +
         CASE WHEN mutuelle_nom IS NOT NULL AND mutuelle_nom <> '' THEN 1 ELSE 0 END +
         CASE WHEN patient_id_rdv IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN patient_id_labo IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN patient_id_fact IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN source_principale IS NOT NULL THEN 1 ELSE 0 END)
    INTO v_nb_champs_remplis
    FROM mdm.mdm_patient
    WHERE patient_id_mdm = p_patient_id;
    
    v_completude := (v_nb_champs_remplis::DECIMAL / v_nb_champs_total) * 100;
    
    -- Score final (ici simplifié, peut être enrichi avec d'autres dimensions)
    v_score := v_completude;
    
    RETURN ROUND(v_score, 2);
END;
$$ LANGUAGE plpgsql;


-- ============================================================
-- TRIGGER: Mise à jour automatique du score de qualité
-- ============================================================

CREATE OR REPLACE FUNCTION mdm.trigger_update_score_qualite()
RETURNS TRIGGER AS $$
BEGIN
    NEW.score_qualite := mdm.calculer_score_qualite_patient(NEW.patient_id_mdm);
    NEW.date_derniere_maj := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_patient_score_qualite
    BEFORE INSERT OR UPDATE ON mdm.mdm_patient
    FOR EACH ROW
    EXECUTE FUNCTION mdm.trigger_update_score_qualite();


-- ============================================================
-- DONNÉES DE RÉFÉRENCE (LOOKUP TABLES)
-- ============================================================

-- Table des spécialités médicales
CREATE TABLE IF NOT EXISTS mdm.ref_specialites (
    code_specialite VARCHAR(20) PRIMARY KEY,
    libelle_specialite VARCHAR(100) NOT NULL,
    categorie VARCHAR(50)
);

INSERT INTO mdm.ref_specialites VALUES
    ('CARDIO', 'Cardiologie', 'Médecine'),
    ('PEDIATR', 'Pédiatrie', 'Médecine'),
    ('RADIO', 'Radiologie', 'Imagerie'),
    ('CHIR', 'Chirurgie générale', 'Chirurgie'),
    ('GYNECO', 'Gynécologie', 'Médecine'),
    ('DERMATO', 'Dermatologie', 'Médecine'),
    ('OPHTALMO', 'Ophtalmologie', 'Médecine');


-- ============================================================
-- GRANTS (Sécurité)
-- ============================================================

-- Créer un rôle pour les applications consommatrices
CREATE ROLE mdm_consumer;
GRANT USAGE ON SCHEMA mdm TO mdm_consumer;
GRANT SELECT ON ALL TABLES IN SCHEMA mdm TO mdm_consumer;

-- Créer un rôle pour les processus ETL
CREATE ROLE mdm_etl;
GRANT USAGE ON SCHEMA mdm TO mdm_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA mdm TO mdm_etl;


-- ============================================================
-- CONFIRMATION
-- ============================================================

SELECT 'Tables MDM créées avec succès!' AS status,
       COUNT(*) AS nombre_tables
FROM information_schema.tables
WHERE table_schema = 'mdm';