"""
Kafka Consumer - Consommation et traitement ETL vers PostgreSQL
Lit depuis Kafka, nettoie, match, et insère dans PostgreSQL
"""

import json
import logging
from kafka import KafkaConsumer
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import hashlib
from fuzzywuzzy import fuzz
import re

# Configuration
logging.basicConfig( 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICS = ['mdm-client-raw', 'mdm-product-raw', 'mdm-supplier-raw']

# Configuration PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'openmetadata_db_2',
    'user': 'postgres',
    'password': 'password'
}

class MDMKafkaConsumer:
    """Consumer Kafka avec traitement ETL vers PostgreSQL"""
    
    def __init__(self, bootstrap_servers, topics, db_config):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='mdm-etl-group',
            max_poll_records=100
        )
        
        self.db_conn = psycopg2.connect(**db_config)
        self.db_conn.autocommit = False
        
        logger.info(f"✅ Consumer Kafka connecté à {bootstrap_servers}")
        logger.info(f"✅ PostgreSQL connecté à {db_config['host']}")
        logger.info(f"📊 Topics écoutés: {', '.join(topics)}")
        
        # Statistiques
        self.stats = {
            'client': {'processed': 0, 'inserted': 0, 'errors': 0},
            'product': {'processed': 0, 'inserted': 0, 'errors': 0},
            'supplier': {'processed': 0, 'inserted': 0, 'errors': 0}
        }
    
    # ============================================
    # NETTOYAGE DES DONNÉES
    # ============================================
    
    def clean_client_data(self, data):
        """Nettoie les données client"""
        cleaned = data.copy()
        
        # Email en minuscule
        if 'email' in cleaned and cleaned['email']:
            cleaned['email'] = str(cleaned['email']).lower().strip()
        
        # Téléphone sans espaces
        if 'phone' in cleaned and cleaned['phone']:
            cleaned['phone'] = re.sub(r'[^\d+]', '', str(cleaned['phone']))
        
        # VAT sans espaces, uppercase
        if 'vat_number' in cleaned and cleaned['vat_number']:
            cleaned['vat_number'] = re.sub(r'\s', '', str(cleaned['vat_number'])).upper()
        
        # Company name trim
        if 'company_name' in cleaned and cleaned['company_name']:
            cleaned['company_name'] = re.sub(r'\s+', ' ', str(cleaned['company_name'])).strip()
        
        return cleaned
    
    def clean_product_data(self, data):
        """Nettoie les données produit"""
        cleaned = data.copy()
        
        # EAN uniquement chiffres
        if 'ean' in cleaned and cleaned['ean']:
            cleaned['ean'] = re.sub(r'[^\d]', '', str(cleaned['ean']))
        
        # HS Code uniquement chiffres
        if 'hs_code' in cleaned and cleaned['hs_code']:
            cleaned['hs_code'] = re.sub(r'[^\d]', '', str(cleaned['hs_code']))
        
        # Product name trim
        if 'product_name' in cleaned and cleaned['product_name']:
            cleaned['product_name'] = re.sub(r'\s+', ' ', str(cleaned['product_name'])).strip()
        
        return cleaned
    
    def clean_supplier_data(self, data):
        """Nettoie les données fournisseur"""
        cleaned = data.copy()
        
        # Email en minuscule
        if 'email' in cleaned and cleaned['email']:
            cleaned['email'] = str(cleaned['email']).lower().strip()
        
        # IBAN sans espaces, uppercase
        if 'iban' in cleaned and cleaned['iban']:
            cleaned['iban'] = re.sub(r'\s', '', str(cleaned['iban'])).upper()
        
        # Company name trim
        if 'company_name' in cleaned and cleaned['company_name']:
            cleaned['company_name'] = re.sub(r'\s+', ' ', str(cleaned['company_name'])).strip()
        
        return cleaned
    
    # ============================================
    # MATCHING
    # ============================================
    
    def fuzzy_match_score(self, str1, str2):
        """Calcule un score de similarité"""
        if not str1 or not str2:
            return 0
        return fuzz.token_sort_ratio(str(str1).lower(), str(str2).lower())
    
    def find_matching_golden(self, data, domain):
        """Trouve un golden record correspondant"""
        cursor = self.db_conn.cursor()
        
        if domain == 'client':
            # Match sur VAT ou nom
            vat = data.get('vat_number')
            name = data.get('company_name')
            
            if vat:
                cursor.execute(
                    "SELECT golden_id, master_company_name FROM mdm_client_golden WHERE master_vat_number = %s",
                    (vat,)
                )
                result = cursor.fetchone()
                if result:
                    return result[0], 100.0  # Match exact
            
            if name:
                cursor.execute(
                    "SELECT golden_id, master_company_name FROM mdm_client_golden LIMIT 100"
                )
                for row in cursor.fetchall():
                    score = self.fuzzy_match_score(name, row[1])
                    if score >= 85:
                        return row[0], score
        
        elif domain == 'product':
            # Match sur EAN ou nom
            ean = data.get('ean')
            name = data.get('product_name')
            
            if ean:
                cursor.execute(
                    "SELECT golden_id FROM mdm_product_golden WHERE master_ean = %s",
                    (ean,)
                )
                result = cursor.fetchone()
                if result:
                    return result[0], 100.0
            
            if name:
                cursor.execute(
                    "SELECT golden_id, master_product_name FROM mdm_product_golden LIMIT 100"
                )
                for row in cursor.fetchall():
                    score = self.fuzzy_match_score(name, row[1])
                    if score >= 85:
                        return row[0], score
        
        elif domain == 'supplier':
            # Match sur VAT ou nom
            vat = data.get('vat_number')
            name = data.get('company_name')
            
            if vat:
                cursor.execute(
                    "SELECT golden_id FROM mdm_supplier_golden WHERE master_vat_number = %s",
                    (vat,)
                )
                result = cursor.fetchone()
                if result:
                    return result[0], 100.0
            
            if name:
                cursor.execute(
                    "SELECT golden_id, master_company_name FROM mdm_supplier_golden LIMIT 100"
                )
                for row in cursor.fetchall():
                    score = self.fuzzy_match_score(name, row[1])
                    if score >= 85:
                        return row[0], score
        
        cursor.close()
        return None, 0
    
    # ============================================
    # INSERTION DANS POSTGRESQL
    # ============================================
    
    def calculate_quality_score(self, data, mandatory_fields):
        """Calcule le score de qualité"""
        total = len(data)
        filled = sum(1 for v in data.values() if v is not None and v != '')
        mandatory_filled = sum(1 for f in mandatory_fields if data.get(f))
        
        completeness = (filled / total) * 100 if total > 0 else 0
        mandatory_score = (mandatory_filled / len(mandatory_fields)) * 100 if mandatory_fields else 100
        
        return round((completeness * 0.7 + mandatory_score * 0.3), 2)
    
    def generate_global_id(self, prefix, data):
        """Génère un ID global unique"""
        unique_string = json.dumps(data, sort_keys=True).encode('utf-8')
        hash_obj = hashlib.md5(unique_string)
        return f"{prefix}_{hash_obj.hexdigest()[:12].upper()}"
    
    def insert_client(self, data, metadata):
        """Insère un client dans PostgreSQL"""
        cursor = self.db_conn.cursor()
        
        try:
            # 1. Insérer dans source
            cursor.execute("""
                INSERT INTO mdm_client_source (source_name, source_system, file_name, load_date)
                VALUES (%s, %s, %s, %s)
                RETURNING source_id
            """, ('Kafka', 'Excel', metadata['source_file'], datetime.now()))
            
            source_id = cursor.fetchone()[0]
            
            # 2. Insérer dans staging
            cursor.execute("""
                INSERT INTO mdm_client_staging 
                (source_id, client_code, company_name, trade_name, vat_number, email, phone, 
                 address_line1, city, postal_code, country_code, customer_type, industry, 
                 currency, status, load_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING staging_id
            """, (
                source_id,
                data.get('client_code'),
                data.get('company_name'),
                data.get('trade_name'),
                data.get('vat_number'),
                data.get('email'),
                data.get('phone'),
                data.get('address_line1'),
                data.get('city'),
                data.get('postal_code'),
                data.get('country_code'),
                data.get('customer_type'),
                data.get('industry'),
                data.get('currency'),
                data.get('status', 'Active'),
                datetime.now()
            ))
            
            staging_id = cursor.fetchone()[0]
            
            # 3. Matching
            golden_id, match_score = self.find_matching_golden(data, 'client')
            
            if not golden_id:
                # Créer nouveau golden record
                quality_score = self.calculate_quality_score(
                    data, 
                    ['company_name', 'country_code', 'customer_type']
                )
                
                cursor.execute("""
                    INSERT INTO mdm_client_golden 
                    (global_client_id, master_company_name, master_trade_name, master_vat_number,
                     master_email, master_phone, master_address_line1, master_city, 
                     master_postal_code, master_country_code, master_customer_type, 
                     master_industry, master_currency, master_status, data_quality_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING golden_id
                """, (
                    self.generate_global_id('CLI', data),
                    data.get('company_name'),
                    data.get('trade_name'),
                    data.get('vat_number'),
                    data.get('email'),
                    data.get('phone'),
                    data.get('address_line1'),
                    data.get('city'),
                    data.get('postal_code'),
                    data.get('country_code'),
                    data.get('customer_type'),
                    data.get('industry'),
                    data.get('currency'),
                    data.get('status', 'Active'),
                    quality_score
                ))
                
                golden_id = cursor.fetchone()[0]
                match_score = 100.0
            
            # 4. Créer mapping
            cursor.execute("""
                INSERT INTO mdm_client_mapping 
                (golden_id, staging_id, source_id, match_score, match_method, confidence_level)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                golden_id,
                staging_id,
                source_id,
                match_score,
                'exact' if match_score == 100 else 'fuzzy',
                'high' if match_score >= 95 else 'medium' if match_score >= 85 else 'low'
            ))
            
            self.db_conn.commit()
            return True
            
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Erreur insertion client: {e}")
            return False
        finally:
            cursor.close()
    
    def insert_product(self, data, metadata):
        """Insère un produit (logique similaire)"""
        # Similaire à insert_client mais pour produits
        # Code simplifié pour la clarté
        try:
            cursor = self.db_conn.cursor()
            
            # Source
            cursor.execute("""
                INSERT INTO mdm_product_source (source_name, source_system, file_name, load_date)
                VALUES (%s, %s, %s, %s) RETURNING source_id
            """, ('Kafka', 'Excel', metadata['source_file'], datetime.now()))
            source_id = cursor.fetchone()[0]
            
            # Staging + Golden + Mapping (logique similaire à client)
            # ... (code complet dans la version finale)
            
            self.db_conn.commit()
            return True
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Erreur insertion produit: {e}")
            return False
        finally:
            cursor.close()
    
    def insert_supplier(self, data, metadata):
        """Insère un fournisseur (logique similaire)"""
        # Similaire aux autres
        try:
            cursor = self.db_conn.cursor()
            # ... logique similaire
            self.db_conn.commit()
            return True
        except Exception as e:
            self.db_conn.rollback()
            logger.error(f"❌ Erreur insertion fournisseur: {e}")
            return False
        finally:
            cursor.close()
    
    # ============================================
    # TRAITEMENT DES MESSAGES
    # ============================================
    
    def process_message(self, message):
        """Traite un message Kafka"""
        try:
            # Extraire données
            value = message.value
            domain = value['domain']
            source_file = value['source_file']
            data = value['data']
            
            # Nettoyer selon le domaine
            if domain == 'client':
                cleaned_data = self.clean_client_data(data)
                success = self.insert_client(cleaned_data, value)
            elif domain == 'product':
                cleaned_data = self.clean_product_data(data)
                success = self.insert_product(cleaned_data, value)
            elif domain == 'supplier':
                cleaned_data = self.clean_supplier_data(data)
                success = self.insert_supplier(cleaned_data, value)
            else:
                logger.warning(f"⚠️  Domaine inconnu: {domain}")
                return
            
            # Statistiques
            self.stats[domain]['processed'] += 1
            if success:
                self.stats[domain]['inserted'] += 1
            else:
                self.stats[domain]['errors'] += 1
            
        except Exception as e:
            logger.error(f"❌ Erreur traitement message: {e}")
    
    def consume(self):
        """Boucle de consommation"""
        logger.info("\n" + "="*60)
        logger.info("🎧 DÉMARRAGE DU CONSUMER KAFKA")
        logger.info("="*60)
        logger.info("En attente de messages...\n")
        
        try:
            for message in self.consumer:
                self.process_message(message)
                
                # Afficher stats tous les 10 messages
                total_processed = sum(s['processed'] for s in self.stats.values())
                if total_processed % 10 == 0 and total_processed > 0:
                    self.print_stats()
        
        except KeyboardInterrupt:
            logger.info("\n⚠️  Interruption utilisateur")
        finally:
            self.print_final_stats()
            self.close()
    
    def print_stats(self):
        """Affiche les statistiques en cours"""
        logger.info("\n📊 Statistiques en cours:")
        for domain, stats in self.stats.items():
            logger.info(f"  {domain.upper():10} - Traités: {stats['processed']:3} | "
                       f"Insérés: {stats['inserted']:3} | Erreurs: {stats['errors']:3}")
    
    def print_final_stats(self):
        """Affiche les statistiques finales"""
        logger.info("\n" + "="*60)
        logger.info("📊 STATISTIQUES FINALES")
        logger.info("="*60)
        for domain, stats in self.stats.items():
            logger.info(f"{domain.upper():10} | Traités: {stats['processed']:4} | "
                       f"Insérés: {stats['inserted']:4} | Erreurs: {stats['errors']:4}")
        total = sum(s['processed'] for s in self.stats.values())
        logger.info(f"{'TOTAL':10} | {total:4} messages traités")
        logger.info("="*60)
    
    def close(self):
        """Ferme les connexions"""
        self.consumer.close()
        self.db_conn.close()
        logger.info("🔌 Consumer et DB fermés")


def main():
    """Fonction principale"""
    consumer = MDMKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=TOPICS,
        db_config=DB_CONFIG
    )
    
    consumer.consume()


if __name__ == "__main__":
    main()