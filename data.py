"""
Générateur de fichiers Excel de test pour le projet MDM
Crée des fichiers sources avec doublons intentionnels et variations
"""

import pandas as pd
import random
from datetime import datetime, timedelta
import os

# Configuration
OUTPUT_DIR = './excel_sources/'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================
# DONNÉES DE RÉFÉRENCE
# ============================================

# Noms d'entreprises réelles
COMPANIES_FR = [
    "CARREFOUR FRANCE SAS", "AUCHAN HOLDING", "SYSTÈME U", 
    "INTERMARCHÉ", "LECLERC", "MONOPRIX", "CASINO GROUPE",
    "METRO FRANCE", "CORA", "MATCH"
]

COMPANIES_ES = [
    "MERCADONA SA", "EL CORTE INGLES", "DIA", "EROSKI",
    "ALCAMPO", "CARREFOUR ESPAÑA", "LIDL ESPAÑA", "ALDI SUR"
]

COMPANIES_IT = [
    "COOP ITALIA", "CONAD", "ESSELUNGA", "CARREFOUR ITALIA",
    "LIDL ITALIA", "IPER", "PLENITUDE RETAIL"
]

# Produits alimentaires
PRODUCTS = [
    ("Organic Olive Oil", "Huile d'Olive Bio", "Aceite de Oliva Ecológico", "Food", "Oils"),
    ("Red Wine Bordeaux", "Vin Rouge Bordeaux", "Vino Tinto Burdeos", "Beverages", "Wine"),
    ("Dark Chocolate 70%", "Chocolat Noir 70%", "Chocolate Negro 70%", "Food", "Chocolate"),
    ("Jamón Ibérico", "Jambon Ibérique", "Prosciutto Iberico", "Food", "Meat"),
    ("Manchego Cheese", "Fromage Manchego", "Queso Manchego", "Food", "Cheese"),
    ("Pasta Penne", "Pâtes Penne", "Pasta Penne", "Food", "Pasta"),
    ("Tomato Sauce", "Sauce Tomate", "Salsa de Tomate", "Food", "Sauces"),
    ("Coffee Beans", "Grains de Café", "Granos de Café", "Beverages", "Coffee"),
    ("Orange Juice", "Jus d'Orange", "Zumo de Naranja", "Beverages", "Juices"),
    ("Mineral Water", "Eau Minérale", "Agua Mineral", "Beverages", "Water")
]

BRANDS = ["Terra Verde", "Château Margaux", "Lindt", "Cinco Jotas", "Garcia Baquero",
          "Barilla", "Mutti", "Lavazza", "Tropicana", "Evian"]

# Fournisseurs
SUPPLIERS = [
    ("DANONE SA", "Manufacturer", "FRA"),
    ("NESTLÉ SA", "Manufacturer", "CHE"),
    ("LACTALIS GROUPE", "Manufacturer", "FRA"),
    ("PERNOD RICARD", "Manufacturer", "FRA"),
    ("UNILEVER NV", "Manufacturer", "NLD"),
    ("COCA-COLA EUROPEAN", "Distributor", "GBR"),
    ("HEINEKEN NV", "Manufacturer", "NLD"),
    ("FERRERO", "Manufacturer", "ITA"),
    ("BARILLA", "Manufacturer", "ITA"),
    ("BONDUELLE", "Manufacturer", "FRA")
]

INDUSTRIES = ["Food & Beverage", "Consumer Goods", "Retail", "Wholesale"]
PAYMENT_TERMS = ["NET 30", "NET 45", "NET 60", "NET 90"]
INCOTERMS = ["EXW", "FCA", "FOB", "CIF", "DDP", "DAP"]
CERTIFICATIONS = ["ISO 9001", "ISO 14001", "FSSC 22000", "ISO 22000", "BRC"]

# ============================================
# GÉNÉRATEUR CLIENT
# ============================================

def generate_client_data(country, num_records=100):
    """Génère des données clients pour un pays"""
    
    if country == 'FRA':
        companies = COMPANIES_FR
        country_code = 'FRA'
        vat_prefix = 'FR'
        city_pool = ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice', 'Nantes']
    elif country == 'ESP':
        companies = COMPANIES_ES
        country_code = 'ESP'
        vat_prefix = 'ES'
        city_pool = ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao']
    else:  # ITA
        companies = COMPANIES_IT
        country_code = 'ITA'
        vat_prefix = 'IT'
        city_pool = ['Roma', 'Milano', 'Napoli', 'Torino', 'Firenze']
    
    data = []
    
    for i in range(num_records):
        company_name = random.choice(companies)
        
        # Variations intentionnelles pour tester le matching
        if random.random() < 0.2:  # 20% de chances de variation
            company_name = company_name.replace(' SAS', '').replace(' SA', '').replace(' HOLDING', '')
        
        record = {
            'client_code': f'{vat_prefix}{str(i+1).zfill(3)}',
            'company_name': company_name,
            'trade_name': company_name.split()[0] if len(company_name.split()) > 0 else company_name,
            'vat_number': f'{vat_prefix}{random.randint(10000000, 99999999):08d}{random.randint(1,9)}',
            'siret': f'{random.randint(10000000, 99999999):08d}{random.randint(10000, 99999):05d}' if country == 'FRA' else None,
            'email': f'contact@{company_name.split()[0].lower()}.{country.lower()}',
            'phone': f'+{random.choice([33, 34, 39])}{random.randint(100000000, 999999999)}',
            'address_line1': f'{random.randint(1, 500)} {random.choice(["Avenue", "Rue", "Boulevard"])} {random.choice(["de Paris", "des Champs", "du Commerce"])}',
            'address_line2': random.choice([None, f'Bâtiment {chr(65+random.randint(0,5))}']),
            'city': random.choice(city_pool),
            'postal_code': f'{random.randint(10000, 99999)}',
            'country_code': country_code,
            'country_name': {'FRA': 'France', 'ESP': 'Espagne', 'ITA': 'Italie'}[country],
            'customer_type': random.choice(['B2B', 'B2B', 'B2B', 'Distributeur']),  # Plus de B2B
            'industry': 'Retail',
            'currency': 'EUR',
            'payment_terms': random.choice(PAYMENT_TERMS),
            'credit_limit': random.choice([300000, 500000, 750000, 1000000]),
            'status': 'Active',
            'created_date': (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
        }
        
        # Données manquantes volontaires (10% de chance)
        if random.random() < 0.1:
            record['email'] = None
        if random.random() < 0.1:
            record['address_line2'] = None
        
        data.append(record)
    
    return pd.DataFrame(data)

# ============================================
# GÉNÉRATEUR PRODUIT
# ============================================

def generate_product_data(country, num_records=100):
    """Génère des données produits pour un pays"""
    
    country_code = {'FRA': 'FRA', 'ESP': 'ESP', 'ITA': 'ITA'}[country]
    
    data = []
    
    for i in range(num_records):
        product = random.choice(PRODUCTS)
        brand = random.choice(BRANDS)
        
        # EAN unique
        ean = f'37601234{(50000 + i):05d}'
        
        record = {
            'product_code': f'PRD{str(i+1).zfill(3)}',
            'sku': f'SKU-{country}-{str(i+1).zfill(3)}',
            'ean': ean,
            'gtin': ean,
            'product_name': product[0],
            'product_name_en': product[0],
            'product_name_fr': product[1],
            'product_name_es': product[2],
            'description': f'High quality {product[0].lower()} from {brand}',
            'category': product[3],
            'sub_category': product[4],
            'brand': brand,
            'manufacturer': brand,
            'country_origin': random.choice(['FRA', 'ESP', 'ITA', 'DEU', 'NLD']),
            'hs_code': f'{random.randint(1000, 9999)}{random.randint(1000, 9999)}',
            'unit_of_measure': random.choice(['UNIT', 'KG', 'LITER', 'BOX']),
            'weight': round(random.uniform(0.1, 5.0), 3),
            'weight_unit': 'KG',
            'dimensions': f'{random.randint(10,50)}x{random.randint(10,50)}x{random.randint(5,30)}cm',
            'unit_price': round(random.uniform(2.5, 50.0), 2),
            'currency': 'EUR',
            'stock_status': random.choice(['In Stock', 'Available', 'Low Stock']),
            'min_order_qty': random.choice([1, 6, 12, 24]),
            'lead_time_days': random.randint(3, 15),
            'is_active': True,
            'created_date': (datetime.now() - timedelta(days=random.randint(10, 200))).strftime('%Y-%m-%d')
        }
        
        # Doublons intentionnels - même produit, codes différents
        if i < 3:  # Les 3 premiers produits seront dupliqués
            record['product_name'] = PRODUCTS[i][0]
            record['ean'] = f'37601234{(56789 + i):05d}'  # EAN différent mais produit similaire
        
        # Données manquantes
        if random.random() < 0.15:
            record['description'] = None
        if random.random() < 0.1:
            record['gtin'] = None
        
        data.append(record)
    
    return pd.DataFrame(data)

# ============================================
# GÉNÉRATEUR FOURNISSEUR
# ============================================

def generate_supplier_data(source_name, num_records=20):
    """Génère des données fournisseurs"""
    
    data = []
    
    for i in range(num_records):
        supplier = random.choice(SUPPLIERS)
        company_name = supplier[0]
        supplier_type = supplier[1]
        country = supplier[2]
        
        # Variations de nom
        if random.random() < 0.2:
            company_name = company_name.replace(' SA', '').replace(' NV', '').replace(' GROUPE', '')
        
        record = {
            'supplier_code': f'SUP{str(i+1).zfill(3)}',
            'company_name': company_name,
            'trade_name': company_name.split()[0],
            'vat_number': f'{country}{random.randint(10000000, 99999999):08d}{chr(65+random.randint(0,25))}',
            'registration_number': f'{random.randint(100000000, 999999999)}',
            'email': f'suppliers@{company_name.split()[0].lower()}.com',
            'phone': f'+{random.randint(30, 49)}{random.randint(100000000, 999999999)}',
            'website': f'www.{company_name.split()[0].lower()}.com',
            'address_line1': f'{random.randint(1, 200)} {random.choice(["Street", "Avenue", "Boulevard"])} {random.choice(["Main", "Industrial", "Business"])}',
            'address_line2': f'Floor {random.randint(1, 10)}' if random.random() > 0.5 else None,
            'city': random.choice(['Paris', 'Amsterdam', 'Brussels', 'Berlin', 'Milano']),
            'postal_code': f'{random.randint(10000, 99999)}',
            'country_code': country,
            'country_name': {'FRA': 'France', 'CHE': 'Switzerland', 'NLD': 'Netherlands', 
                           'GBR': 'United Kingdom', 'ITA': 'Italy', 'DEU': 'Germany'}[country],
            'supplier_type': supplier_type,
            'industry': random.choice(INDUSTRIES),
            'payment_terms': random.choice(PAYMENT_TERMS),
            'incoterms': random.choice(INCOTERMS),
            'certification': ', '.join(random.sample(CERTIFICATIONS, random.randint(1, 3))),
            'rating': round(random.uniform(3.5, 5.0), 1),
            'currency': 'EUR' if country in ['FRA', 'ITA', 'NLD', 'DEU'] else random.choice(['EUR', 'CHF', 'GBP']),
            'bank_name': f'{random.choice(["BNP Paribas", "Credit Suisse", "ING", "Deutsche Bank", "Santander"])}',
            'iban': f'{country}{"".join([str(random.randint(0,9)) for _ in range(20)])}',
            'swift_bic': f'{company_name[:4].upper()}{country}{chr(65+random.randint(0,25))}{chr(65+random.randint(0,25))}',
            'status': 'Active',
            'created_date': (datetime.now() - timedelta(days=random.randint(60, 500))).strftime('%Y-%m-%d')
        }
        
        # Doublons intentionnels
        if i < 2:  # Les 2 premiers seront des doublons potentiels
            record['company_name'] = SUPPLIERS[i][0]
            record['supplier_code'] = f'SUP{random.randint(100, 999)}'  # Code différent
        
        # Données manquantes
        if random.random() < 0.1:
            record['website'] = None
        if random.random() < 0.15:
            record['certification'] = None
        
        data.append(record)
    
    return pd.DataFrame(data)

# ============================================
# GÉNÉRATION DES FICHIERS
# ============================================

def generate_all_files():
    """Génère tous les fichiers Excel de test"""
    
    print("🚀 Génération des fichiers Excel de test MDM...")
    print("="*60)
    
    # CLIENTS
    print("\n👥 GÉNÉRATION DES DONNÉES CLIENT...")
    
    client_fr = generate_client_data('FRA')
    filename = f'{OUTPUT_DIR}CLIENT_SOURCE_1_FRANCE.xlsx'
    client_fr.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(client_fr)} enregistrements")
    
    client_es = generate_client_data('ESP')
    filename = f'{OUTPUT_DIR}CLIENT_SOURCE_2_SPAIN.xlsx'
    client_es.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(client_es)} enregistrements")
    
    client_it = generate_client_data('ITA')
    filename = f'{OUTPUT_DIR}CLIENT_SOURCE_3_ITALY.xlsx'
    client_it.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(client_it)} enregistrements")
    
    # PRODUITS
    print("\n📦 GÉNÉRATION DES DONNÉES PRODUIT...")
    
    product_fr = generate_product_data('FRA')
    filename = f'{OUTPUT_DIR}PRODUCT_SOURCE_1_FRANCE.xlsx'
    product_fr.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(product_fr)} enregistrements")
    
    product_es = generate_product_data('ESP')
    filename = f'{OUTPUT_DIR}PRODUCT_SOURCE_2_SPAIN.xlsx'
    product_es.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(product_es)} enregistrements")
    
    product_it = generate_product_data('ITA')
    filename = f'{OUTPUT_DIR}PRODUCT_SOURCE_3_ITALY.xlsx'
    product_it.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(product_it)} enregistrements")
    
    # FOURNISSEURS
    print("\n🏭 GÉNÉRATION DES DONNÉES FOURNISSEUR...")
    
    supplier_fr = generate_supplier_data('FRANCE')
    filename = f'{OUTPUT_DIR}SUPPLIER_SOURCE_1_FRANCE.xlsx'
    supplier_fr.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(supplier_fr)} enregistrements")
    
    supplier_int = generate_supplier_data('INTERNATIONAL')
    filename = f'{OUTPUT_DIR}SUPPLIER_SOURCE_2_INTERNATIONAL.xlsx'
    supplier_int.to_excel(filename, index=False)
    print(f"✅ {filename} - {len(supplier_int)} enregistrements")
    
    print("\n" + "="*60)
    print("✅ GÉNÉRATION TERMINÉE !")
    print(f"📁 Fichiers créés dans : {OUTPUT_DIR}")
    print("\n📊 STATISTIQUES :")
    print(f"   - Clients : {len(client_fr) + len(client_es) + len(client_it)} enregistrements")
    print(f"   - Produits : {len(product_fr) + len(product_es) + len(product_it)} enregistrements")
    print(f"   - Fournisseurs : {len(supplier_fr) + len(supplier_int)} enregistrements")
    print("\n⚠️  Note : Les fichiers contiennent volontairement :")
    print("   • Des doublons pour tester le matching")
    print("   • Des données manquantes pour tester la qualité")
    print("   • Des variations de noms pour tester le fuzzy matching")

if __name__ == "__main__":
    generate_all_files()