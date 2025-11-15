-- ============================================
-- SCHEMA MDM POUR SOCIÉTÉ DE COMMERCE INTERNATIONAL
-- ============================================

-- ============================================
-- 1. MDM CLIENT
-- ============================================

-- Table des sources clients
CREATE TABLE mdm_client_source (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_country VARCHAR(3),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE
);

-- Table staging des clients (données brutes)
CREATE TABLE mdm_client_staging (
    staging_id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES mdm_client_source(source_id),
    client_code VARCHAR(50),
    company_name VARCHAR(255),
    trade_name VARCHAR(255),
    vat_number VARCHAR(50),
    siret VARCHAR(20),
    email VARCHAR(255),
    phone VARCHAR(50),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    country_code VARCHAR(3),
    country_name VARCHAR(100),
    customer_type VARCHAR(50), -- B2B, B2C, Distributeur
    industry VARCHAR(100),
    currency VARCHAR(3),
    payment_terms VARCHAR(50),
    credit_limit DECIMAL(15,2), 
    status VARCHAR(20),
    created_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Golden Record Client (Master Data)
CREATE TABLE mdm_client_golden (
    golden_id SERIAL PRIMARY KEY,
    global_client_id VARCHAR(50) UNIQUE NOT NULL, -- Identifiant unique global
    master_company_name VARCHAR(255) NOT NULL,
    master_trade_name VARCHAR(255),
    master_vat_number VARCHAR(50),
    master_email VARCHAR(255),
    master_phone VARCHAR(50),
    master_address_line1 VARCHAR(255),
    master_address_line2 VARCHAR(255),
    master_city VARCHAR(100),
    master_postal_code VARCHAR(20),
    master_country_code VARCHAR(3),
    master_customer_type VARCHAR(50),
    master_industry VARCHAR(100),
    master_currency VARCHAR(3),
    master_status VARCHAR(20),
    data_quality_score DECIMAL(5,2), -- Score de qualité 0-100
    completeness_score DECIMAL(5,2),
    last_verification_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de mapping (sources → golden)
CREATE TABLE mdm_client_mapping (
    mapping_id SERIAL PRIMARY KEY,
    golden_id INTEGER REFERENCES mdm_client_golden(golden_id),
    staging_id INTEGER REFERENCES mdm_client_staging(staging_id),
    source_id INTEGER REFERENCES mdm_client_source(source_id),
    match_score DECIMAL(5,2), -- Score de matching
    match_method VARCHAR(50), -- exact, fuzzy, manual
    confidence_level VARCHAR(20), -- high, medium, low
    is_primary_source BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 2. MDM PRODUIT
-- ============================================

-- Table des sources produits
CREATE TABLE mdm_product_source (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_country VARCHAR(3),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE
);

-- Table staging des produits
CREATE TABLE mdm_product_staging (
    staging_id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES mdm_product_source(source_id),
    product_code VARCHAR(100),
    sku VARCHAR(100),
    ean VARCHAR(20),
    gtin VARCHAR(20),
    product_name VARCHAR(255),
    product_name_en VARCHAR(255),
    product_name_fr VARCHAR(255),
    product_name_es VARCHAR(255),
    description TEXT,
    category VARCHAR(100),
    sub_category VARCHAR(100),
    brand VARCHAR(100),
    manufacturer VARCHAR(100),
    country_origin VARCHAR(3),
    hs_code VARCHAR(20), -- Code douanier
    unit_of_measure VARCHAR(20),
    weight DECIMAL(10,3),
    weight_unit VARCHAR(10),
    dimensions VARCHAR(50),
    unit_price DECIMAL(15,2),
    currency VARCHAR(3),
    stock_status VARCHAR(50),
    min_order_qty INTEGER,
    lead_time_days INTEGER,
    is_active BOOLEAN,
    created_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Golden Record Produit
CREATE TABLE mdm_product_golden (
    golden_id SERIAL PRIMARY KEY,
    global_product_id VARCHAR(50) UNIQUE NOT NULL,
    master_product_code VARCHAR(100),
    master_sku VARCHAR(100),
    master_ean VARCHAR(20),
    master_gtin VARCHAR(20),
    master_product_name VARCHAR(255) NOT NULL,
    master_product_name_en VARCHAR(255),
    master_product_name_fr VARCHAR(255),
    master_product_name_es VARCHAR(255),
    master_description TEXT,
    master_category VARCHAR(100),
    master_sub_category VARCHAR(100),
    master_brand VARCHAR(100),
    master_manufacturer VARCHAR(100),
    master_country_origin VARCHAR(3),
    master_hs_code VARCHAR(20),
    master_unit_of_measure VARCHAR(20),
    master_weight DECIMAL(10,3),
    master_weight_unit VARCHAR(10),
    master_dimensions VARCHAR(50),
    master_is_active BOOLEAN,
    data_quality_score DECIMAL(5,2),
    completeness_score DECIMAL(5,2),
    last_verification_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de mapping produits
CREATE TABLE mdm_product_mapping (
    mapping_id SERIAL PRIMARY KEY,
    golden_id INTEGER REFERENCES mdm_product_golden(golden_id),
    staging_id INTEGER REFERENCES mdm_product_staging(staging_id),
    source_id INTEGER REFERENCES mdm_product_source(source_id),
    match_score DECIMAL(5,2),
    match_method VARCHAR(50),
    confidence_level VARCHAR(20),
    is_primary_source BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- 3. MDM FOURNISSEUR
-- ============================================

-- Table des sources fournisseurs
CREATE TABLE mdm_supplier_source (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    source_system VARCHAR(50) NOT NULL,
    source_country VARCHAR(3),
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE
);

-- Table staging des fournisseurs
CREATE TABLE mdm_supplier_staging (
    staging_id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES mdm_supplier_source(source_id),
    supplier_code VARCHAR(50),
    company_name VARCHAR(255),
    trade_name VARCHAR(255),
    vat_number VARCHAR(50),
    registration_number VARCHAR(50),
    email VARCHAR(255),
    phone VARCHAR(50),
    website VARCHAR(255),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    country_code VARCHAR(3),
    country_name VARCHAR(100),
    supplier_type VARCHAR(50), -- Manufacturer, Distributor, Wholesaler
    industry VARCHAR(100),
    payment_terms VARCHAR(50),
    incoterms VARCHAR(10), -- FOB, CIF, EXW, etc.
    certification VARCHAR(255), -- ISO, CE, etc.
    rating DECIMAL(3,2), -- Note fournisseur 0-5
    currency VARCHAR(3),
    bank_name VARCHAR(100),
    iban VARCHAR(50),
    swift_bic VARCHAR(20),
    status VARCHAR(20),
    created_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Golden Record Fournisseur
CREATE TABLE mdm_supplier_golden (
    golden_id SERIAL PRIMARY KEY,
    global_supplier_id VARCHAR(50) UNIQUE NOT NULL,
    master_company_name VARCHAR(255) NOT NULL,
    master_trade_name VARCHAR(255),
    master_vat_number VARCHAR(50),
    master_registration_number VARCHAR(50),
    master_email VARCHAR(255),
    master_phone VARCHAR(50),
    master_website VARCHAR(255),
    master_address_line1 VARCHAR(255),
    master_address_line2 VARCHAR(255),
    master_city VARCHAR(100),
    master_postal_code VARCHAR(20),
    master_country_code VARCHAR(3),
    master_supplier_type VARCHAR(50),
    master_industry VARCHAR(100),
    master_payment_terms VARCHAR(50),
    master_incoterms VARCHAR(10),
    master_certification VARCHAR(255),
    master_rating DECIMAL(3,2),
    master_currency VARCHAR(3),
    master_status VARCHAR(20),
    data_quality_score DECIMAL(5,2),
    completeness_score DECIMAL(5,2),
    risk_score DECIMAL(5,2), -- Score de risque fournisseur
    last_verification_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table de mapping fournisseurs
CREATE TABLE mdm_supplier_mapping (
    mapping_id SERIAL PRIMARY KEY,
    golden_id INTEGER REFERENCES mdm_supplier_golden(golden_id),
    staging_id INTEGER REFERENCES mdm_supplier_staging(staging_id),
    source_id INTEGER REFERENCES mdm_supplier_source(source_id),
    match_score DECIMAL(5,2),
    match_method VARCHAR(50),
    confidence_level VARCHAR(20),
    is_primary_source BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- TABLES TRANSVERSALES (DATA QUALITY & AUDIT)
-- ============================================

-- Table Data Quality Rules
CREATE TABLE mdm_data_quality_rules (
    rule_id SERIAL PRIMARY KEY,
    domain VARCHAR(50) NOT NULL, -- client, product, supplier
    rule_name VARCHAR(100) NOT NULL,
    rule_type VARCHAR(50), -- completeness, validity, consistency, accuracy
    rule_description TEXT,
    sql_expression TEXT,
    severity VARCHAR(20), -- critical, high, medium, low
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table Data Quality Issues
CREATE TABLE mdm_data_quality_issues (
    issue_id SERIAL PRIMARY KEY,
    rule_id INTEGER REFERENCES mdm_data_quality_rules(rule_id),
    domain VARCHAR(50),
    table_name VARCHAR(100),
    record_id INTEGER,
    issue_description TEXT,
    severity VARCHAR(20),
    status VARCHAR(20), -- open, in_progress, resolved, ignored
    detected_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_date TIMESTAMP,
    resolved_by VARCHAR(100)
);

-- Table Audit Log
CREATE TABLE mdm_audit_log (
    audit_id SERIAL PRIMARY KEY,
    domain VARCHAR(50),
    table_name VARCHAR(100),
    record_id INTEGER,
    action VARCHAR(20), -- INSERT, UPDATE, DELETE, MERGE
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_reason TEXT
);

-- Table Data Dictionary
CREATE TABLE mdm_data_dictionary (
    dict_id SERIAL PRIMARY KEY,
    domain VARCHAR(50),
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    business_name VARCHAR(255),
    description TEXT,
    data_type VARCHAR(50),
    is_mandatory BOOLEAN,
    valid_values TEXT,
    validation_rule TEXT,
    source_system VARCHAR(100),
    data_owner VARCHAR(100),
    pii_flag BOOLEAN DEFAULT FALSE, -- Données personnelles
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEX POUR PERFORMANCE
-- ============================================

-- Client indexes
CREATE INDEX idx_client_staging_source ON mdm_client_staging(source_id);
CREATE INDEX idx_client_staging_code ON mdm_client_staging(client_code);
CREATE INDEX idx_client_staging_vat ON mdm_client_staging(vat_number);
CREATE INDEX idx_client_golden_global_id ON mdm_client_golden(global_client_id);
CREATE INDEX idx_client_golden_name ON mdm_client_golden(master_company_name);

-- Product indexes
CREATE INDEX idx_product_staging_source ON mdm_product_staging(source_id);
CREATE INDEX idx_product_staging_code ON mdm_product_staging(product_code);
CREATE INDEX idx_product_staging_ean ON mdm_product_staging(ean);
CREATE INDEX idx_product_golden_global_id ON mdm_product_golden(global_product_id);

-- Supplier indexes
CREATE INDEX idx_supplier_staging_source ON mdm_supplier_staging(source_id);
CREATE INDEX idx_supplier_staging_code ON mdm_supplier_staging(supplier_code);
CREATE INDEX idx_supplier_staging_vat ON mdm_supplier_staging(vat_number);
CREATE INDEX idx_supplier_golden_global_id ON mdm_supplier_golden(global_supplier_id);

-- ============================================
-- VUES UTILES
-- ============================================

-- Vue consolidée Clients avec score qualité
CREATE VIEW v_mdm_client_master AS
SELECT 
    g.*,
    COUNT(DISTINCT m.source_id) as nb_sources,
    AVG(m.match_score) as avg_match_score
FROM mdm_client_golden g
LEFT JOIN mdm_client_mapping m ON g.golden_id = m.golden_id
GROUP BY g.golden_id;

-- Vue consolidée Produits avec score qualité
CREATE VIEW v_mdm_product_master AS
SELECT 
    g.*,
    COUNT(DISTINCT m.source_id) as nb_sources,
    AVG(m.match_score) as avg_match_score
FROM mdm_product_golden g
LEFT JOIN mdm_product_mapping m ON g.golden_id = m.golden_id
GROUP BY g.golden_id;

-- Vue consolidée Fournisseurs avec score qualité
CREATE VIEW v_mdm_supplier_master AS
SELECT 
    g.*,
    COUNT(DISTINCT m.source_id) as nb_sources,
    AVG(m.match_score) as avg_match_score
FROM mdm_supplier_golden g
LEFT JOIN mdm_supplier_mapping m ON g.golden_id = m.golden_id
GROUP BY g.golden_id;