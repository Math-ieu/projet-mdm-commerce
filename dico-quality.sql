-- ============================================
-- RÈGLES DE QUALITÉ DES DONNÉES (DATA QUALITY RULES)
-- ============================================

-- Insertion des règles de qualité pour les 3 domaines MDM

-- ========== RÈGLES CLIENT ==========

INSERT INTO mdm_data_quality_rules (domain, rule_name, rule_type, rule_description, sql_expression, severity) VALUES
('client', 'Client - Nom obligatoire', 'completeness', 'Le nom de l''entreprise est obligatoire', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE master_company_name IS NULL OR master_company_name = ''''', 
 'critical'),

('client', 'Client - Pays obligatoire', 'completeness', 'Le code pays doit être renseigné', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE master_country_code IS NULL', 
 'critical'),

('client', 'Client - Email valide', 'validity', 'L''email doit avoir un format valide', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE master_email IS NOT NULL AND master_email NOT LIKE ''%@%.%''', 
 'high'),

('client', 'Client - VAT format valide', 'validity', 'Le numéro de TVA doit respecter le format pays', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE master_vat_number IS NOT NULL AND LENGTH(master_vat_number) < 8', 
 'high'),

('client', 'Client - Téléphone format valide', 'validity', 'Le téléphone doit commencer par +', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE master_phone IS NOT NULL AND master_phone NOT LIKE ''+%''', 
 'medium'),

('client', 'Client - Type client valide', 'validity', 'Le type client doit être B2B, B2C ou Distributeur', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE master_customer_type NOT IN (''B2B'', ''B2C'', ''Distributeur'')', 
 'high'),

('client', 'Client - Doublons nom+pays', 'consistency', 'Pas de doublons sur nom entreprise + pays', 
 'SELECT master_company_name, master_country_code, COUNT(*) FROM mdm_client_golden GROUP BY master_company_name, master_country_code HAVING COUNT(*) > 1', 
 'critical'),

('client', 'Client - Score qualité minimum', 'accuracy', 'Le score de qualité doit être >= 70', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE data_quality_score < 70', 
 'medium'),

('client', 'Client - Adresse complète', 'completeness', 'Adresse, ville et code postal requis pour clients actifs', 
 'SELECT COUNT(*) FROM mdm_client_golden WHERE master_status = ''Active'' AND (master_address_line1 IS NULL OR master_city IS NULL OR master_postal_code IS NULL)', 
 'high');

-- ========== RÈGLES PRODUIT ==========

INSERT INTO mdm_data_quality_rules (domain, rule_name, rule_type, rule_description, sql_expression, severity) VALUES
('product', 'Produit - Nom obligatoire', 'completeness', 'Le nom du produit est obligatoire', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE master_product_name IS NULL OR master_product_name = ''''', 
 'critical'),

('product', 'Produit - Catégorie obligatoire', 'completeness', 'La catégorie doit être renseignée', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE master_category IS NULL', 
 'critical'),

('product', 'Produit - EAN format valide', 'validity', 'L''EAN doit contenir 13 chiffres', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE master_ean IS NOT NULL AND (LENGTH(master_ean) != 13 OR master_ean !~ ''^[0-9]+$'')', 
 'high'),

('product', 'Produit - Code HS valide', 'validity', 'Le code douanier HS doit contenir 6 à 10 chiffres', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE master_hs_code IS NOT NULL AND (LENGTH(master_hs_code) < 6 OR LENGTH(master_hs_code) > 10)', 
 'high'),

('product', 'Produit - Poids positif', 'validity', 'Le poids doit être supérieur à 0', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE master_weight IS NOT NULL AND master_weight <= 0', 
 'medium'),

('product', 'Produit - Unité de mesure valide', 'validity', 'L''unité de mesure doit être dans la liste autorisée', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE master_unit_of_measure NOT IN (''UNIT'', ''KG'', ''LITER'', ''METER'', ''BOX'', ''PALLET'')', 
 'high'),

('product', 'Produit - Doublons EAN', 'consistency', 'Pas de doublons sur le code EAN', 
 'SELECT master_ean, COUNT(*) FROM mdm_product_golden WHERE master_ean IS NOT NULL GROUP BY master_ean HAVING COUNT(*) > 1', 
 'critical'),

('product', 'Produit - Description multilingue', 'completeness', 'Au moins 2 langues pour le nom du produit', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE (master_product_name_en IS NULL AND master_product_name_fr IS NULL) OR (master_product_name_en IS NULL AND master_product_name_es IS NULL)', 
 'low'),

('product', 'Produit - Pays d''origine ISO', 'validity', 'Le code pays doit être au format ISO 3 lettres', 
 'SELECT COUNT(*) FROM mdm_product_golden WHERE master_country_origin IS NOT NULL AND LENGTH(master_country_origin) != 3', 
 'medium');

-- ========== RÈGLES FOURNISSEUR ==========

INSERT INTO mdm_data_quality_rules (domain, rule_name, rule_type, rule_description, sql_expression, severity) VALUES
('supplier', 'Fournisseur - Nom obligatoire', 'completeness', 'Le nom de l''entreprise est obligatoire', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE master_company_name IS NULL OR master_company_name = ''''', 
 'critical'),

('supplier', 'Fournisseur - Pays obligatoire', 'completeness', 'Le code pays doit être renseigné', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE master_country_code IS NULL', 
 'critical'),

('supplier', 'Fournisseur - Email valide', 'validity', 'L''email doit avoir un format valide', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE master_email IS NOT NULL AND master_email NOT LIKE ''%@%.%''', 
 'high'),

('supplier', 'Fournisseur - Type valide', 'validity', 'Le type doit être Manufacturer, Distributor ou Wholesaler', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE master_supplier_type NOT IN (''Manufacturer'', ''Distributor'', ''Wholesaler'')', 
 'high'),

('supplier', 'Fournisseur - Incoterms valide', 'validity', 'Les Incoterms doivent être standards (FOB, CIF, EXW, etc.)', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE master_incoterms IS NOT NULL AND master_incoterms NOT IN (''EXW'', ''FCA'', ''FOB'', ''CIF'', ''DDP'', ''DAP'', ''CPT'')', 
 'medium'),

('supplier', 'Fournisseur - Rating valide', 'validity', 'La note doit être entre 0 et 5', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE master_rating IS NOT NULL AND (master_rating < 0 OR master_rating > 5)', 
 'high'),

('supplier', 'Fournisseur - IBAN format', 'validity', 'L''IBAN doit commencer par 2 lettres pays', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE master_iban IS NOT NULL AND master_iban !~ ''^[A-Z]{2}[0-9]+$''', 
 'medium'),

('supplier', 'Fournisseur - Doublons VAT', 'consistency', 'Pas de doublons sur le numéro de TVA', 
 'SELECT master_vat_number, COUNT(*) FROM mdm_supplier_golden WHERE master_vat_number IS NOT NULL GROUP BY master_vat_number HAVING COUNT(*) > 1', 
 'critical'),

('supplier', 'Fournisseur - Contact complet', 'completeness', 'Email ou téléphone requis', 
 'SELECT COUNT(*) FROM mdm_supplier_golden WHERE (master_email IS NULL OR master_email = '''') AND (master_phone IS NULL OR master_phone = '''')', 
 'high');

-- ============================================
-- DATA DICTIONARY (DICTIONNAIRE DE DONNÉES)
-- ============================================

-- ========== CLIENT DICTIONARY ==========

INSERT INTO mdm_data_dictionary (domain, table_name, column_name, business_name, description, data_type, is_mandatory, valid_values, data_owner, pii_flag) VALUES
('client', 'mdm_client_golden', 'global_client_id', 'Identifiant Client Global', 'Identifiant unique du client dans le système MDM', 'VARCHAR(50)', TRUE, NULL, 'MDM Team', FALSE),
('client', 'mdm_client_golden', 'master_company_name', 'Raison Sociale', 'Nom légal de l''entreprise cliente', 'VARCHAR(255)', TRUE, NULL, 'Sales Team', FALSE),
('client', 'mdm_client_golden', 'master_trade_name', 'Nom Commercial', 'Nom commercial ou enseigne du client', 'VARCHAR(255)', FALSE, NULL, 'Sales Team', FALSE),
('client', 'mdm_client_golden', 'master_vat_number', 'Numéro TVA', 'Numéro de TVA intracommunautaire', 'VARCHAR(50)', FALSE, 'Format: CC99999999', 'Finance Team', FALSE),
('client', 'mdm_client_golden', 'master_email', 'Email Principal', 'Adresse email principale du contact', 'VARCHAR(255)', FALSE, 'Format email valide', 'Sales Team', TRUE),
('client', 'mdm_client_golden', 'master_phone', 'Téléphone', 'Numéro de téléphone international', 'VARCHAR(50)', FALSE, 'Format: +[code pays][numéro]', 'Sales Team', TRUE),
('client', 'mdm_client_golden', 'master_country_code', 'Code Pays', 'Code pays ISO 3166-1 alpha-3', 'VARCHAR(3)', TRUE, 'FRA, ESP, DEU, ITA, etc.', 'MDM Team', FALSE),
('client', 'mdm_client_golden', 'master_customer_type', 'Type Client', 'Catégorie du client', 'VARCHAR(50)', TRUE, 'B2B, B2C, Distributeur', 'Sales Team', FALSE),
('client', 'mdm_client_golden', 'master_industry', 'Secteur d''Activité', 'Industrie ou secteur du client', 'VARCHAR(100)', FALSE, 'Retail, Food Service, etc.', 'Sales Team', FALSE),
('client', 'mdm_client_golden', 'master_currency', 'Devise', 'Devise de facturation', 'VARCHAR(3)', FALSE, 'EUR, USD, GBP, CHF', 'Finance Team', FALSE),
('client', 'mdm_client_golden', 'data_quality_score', 'Score Qualité', 'Score de qualité des données (0-100)', 'DECIMAL(5,2)', FALSE, '0-100', 'MDM Team', FALSE);

-- ========== PRODUCT DICTIONARY ==========

INSERT INTO mdm_data_dictionary (domain, table_name, column_name, business_name, description, data_type, is_mandatory, valid_values, data_owner, pii_flag) VALUES
('product', 'mdm_product_golden', 'global_product_id', 'Identifiant Produit Global', 'Identifiant unique du produit dans le système MDM', 'VARCHAR(50)', TRUE, NULL, 'MDM Team', FALSE),
('product', 'mdm_product_golden', 'master_product_code', 'Code Produit', 'Code produit interne', 'VARCHAR(100)', FALSE, NULL, 'Product Team', FALSE),
('product', 'mdm_product_golden', 'master_sku', 'SKU', 'Stock Keeping Unit - référence stock', 'VARCHAR(100)', FALSE, NULL, 'Logistics Team', FALSE),
('product', 'mdm_product_golden', 'master_ean', 'Code-barres EAN', 'Code-barres EAN-13', 'VARCHAR(20)', FALSE, '13 chiffres', 'Product Team', FALSE),
('product', 'mdm_product_golden', 'master_product_name', 'Nom Produit', 'Nom du produit (langue principale)', 'VARCHAR(255)', TRUE, NULL, 'Product Team', FALSE),
('product', 'mdm_product_golden', 'master_category', 'Catégorie', 'Catégorie principale du produit', 'VARCHAR(100)', TRUE, 'Food, Beverages, etc.', 'Product Team', FALSE),
('product', 'mdm_product_golden', 'master_brand', 'Marque', 'Marque du produit', 'VARCHAR(100)', FALSE, NULL, 'Marketing Team', FALSE),
('product', 'mdm_product_golden', 'master_hs_code', 'Code Douanier', 'Code du système harmonisé (HS Code)', 'VARCHAR(20)', FALSE, '6-10 chiffres', 'Logistics Team', FALSE),
('product', 'mdm_product_golden', 'master_country_origin', 'Pays d''Origine', 'Pays de fabrication/origine', 'VARCHAR(3)', FALSE, 'Code ISO 3 lettres', 'Product Team', FALSE),
('product', 'mdm_product_golden', 'master_unit_of_measure', 'Unité de Mesure', 'Unité de mesure pour la vente', 'VARCHAR(20)', TRUE, 'UNIT, KG, LITER, etc.', 'Product Team', FALSE),
('product', 'mdm_product_golden', 'master_weight', 'Poids', 'Poids unitaire du produit', 'DECIMAL(10,3)', FALSE, 'Positif', 'Logistics Team', FALSE);

-- ========== SUPPLIER DICTIONARY ==========

INSERT INTO mdm_data_dictionary (domain, table_name, column_name, business_name, description, data_type, is_mandatory, valid_values, data_owner, pii_flag) VALUES
('supplier', 'mdm_supplier_golden', 'global_supplier_id', 'Identifiant Fournisseur Global', 'Identifiant unique du fournisseur dans le système MDM', 'VARCHAR(50)', TRUE, NULL, 'MDM Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_company_name', 'Raison Sociale', 'Nom légal de l''entreprise fournisseur', 'VARCHAR(255)', TRUE, NULL, 'Procurement Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_vat_number', 'Numéro TVA', 'Numéro de TVA du fournisseur', 'VARCHAR(50)', FALSE, 'Format: CC99999999', 'Finance Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_email', 'Email Contact', 'Adresse email du contact principal', 'VARCHAR(255)', FALSE, 'Format email valide', 'Procurement Team', TRUE),
('supplier', 'mdm_supplier_golden', 'master_country_code', 'Code Pays', 'Code pays ISO 3166-1 alpha-3', 'VARCHAR(3)', TRUE, 'FRA, ESP, ITA, etc.', 'MDM Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_supplier_type', 'Type Fournisseur', 'Catégorie du fournisseur', 'VARCHAR(50)', TRUE, 'Manufacturer, Distributor, Wholesaler', 'Procurement Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_payment_terms', 'Conditions Paiement', 'Termes de paiement négociés', 'VARCHAR(50)', FALSE, 'NET 30, NET 45, NET 60', 'Finance Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_incoterms', 'Incoterms', 'Conditions internationales de commerce', 'VARCHAR(10)', FALSE, 'EXW, FOB, CIF, DDP, etc.', 'Logistics Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_certification', 'Certifications', 'Certifications qualité du fournisseur', 'VARCHAR(255)', FALSE, 'ISO 9001, FSSC 22000, etc.', 'Quality Team', FALSE),
('supplier', 'mdm_supplier_golden', 'master_rating', 'Note Fournisseur', 'Évaluation de performance (0-5)', 'DECIMAL(3,2)', FALSE, '0.00 - 5.00', 'Procurement Team', FALSE),
('supplier', 'mdm_supplier_golden', 'risk_score', 'Score Risque', 'Évaluation du risque fournisseur (0-100)', 'DECIMAL(5,2)', FALSE, '0-100 (plus élevé = plus risqué)', 'Risk Management', FALSE);

-- ============================================
-- VUES POUR REPORTING QUALITÉ
-- ============================================

-- Vue: Résumé qualité par domaine
CREATE OR REPLACE VIEW v_data_quality_summary AS
SELECT 
    domain,
    COUNT(*) as total_rules,
    SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_rules,
    SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high_rules,
    SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) as medium_rules,
    SUM(CASE WHEN severity = 'low' THEN 1 ELSE 0 END) as low_rules,
    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_rules
FROM mdm_data_quality_rules
GROUP BY domain;

-- Vue: Issues ouvertes par domaine
CREATE OR REPLACE VIEW v_open_quality_issues AS
SELECT 
    i.domain,
    r.severity,
    COUNT(*) as issue_count
FROM mdm_data_quality_issues i
JOIN mdm_data_quality_rules r ON i.rule_id = r.rule_id
WHERE i.status IN ('open', 'in_progress')
GROUP BY i.domain, r.severity
ORDER BY 
    CASE r.severity 
        WHEN 'critical' THEN 1 
        WHEN 'high' THEN 2 
        WHEN 'medium' THEN 3 
        ELSE 4 
    END;

-- Vue: Data Dictionary complet
CREATE OR REPLACE VIEW v_data_dictionary_full AS
SELECT 
    domain,
    table_name,
    column_name,
    business_name,
    description,
    data_type,
    CASE WHEN is_mandatory THEN 'Oui' ELSE 'Non' END as mandatory,
    valid_values,
    data_owner,
    CASE WHEN pii_flag THEN 'Données Personnelles' ELSE 'Standard' END as data_classification
FROM mdm_data_dictionary
ORDER BY domain, table_name, column_name;