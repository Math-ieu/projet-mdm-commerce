"""
Kafka Producer - Ingestion des fichiers Excel vers Kafka Topics
Lit les fichiers Excel et envoie les données vers Kafka
"""

import pandas as pd
import json
import os
import logging
from kafka import KafkaProducer
from datetime import datetime
import time

# Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
EXCEL_PATH = './excel_sources/'

# Topics Kafka
TOPICS = {
    'client': 'mdm-client-raw',
    'product': 'mdm-product-raw',
    'supplier': 'mdm-supplier-raw'
}

class MDMKafkaProducer:
    """Producer Kafka pour l'ingestion des données MDM"""
    
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Attendre la confirmation de tous les replicas
            retries=3,
            max_in_flight_requests_per_connection=1  # Garantir l'ordre
        )
        logger.info(f"✅ Producer Kafka connecté à {bootstrap_servers}")
    
    def send_record(self, topic, key, value, domain, source_file):
        """Envoie un enregistrement vers Kafka"""
        # Enrichir avec métadonnées
        enriched_value = {
            'domain': domain,
            'source_file': source_file,
            'ingestion_timestamp': datetime.now().isoformat(),
            'data': value
        }
        
        try:
            future = self.producer.send(topic, key=key, value=enriched_value)
            # Attendre la confirmation
            record_metadata = future.get(timeout=10)
            return True
        except Exception as e:
            logger.error(f"❌ Erreur d'envoi: {e}")
            return False
    
    def ingest_client_files(self):
        """Ingère les fichiers clients vers Kafka"""
        logger.info("📂 Ingestion des fichiers CLIENT...")
        
        files = [f for f in os.listdir(EXCEL_PATH) if f.startswith('CLIENT_SOURCE_')]
        total_records = 0
        
        for file in files:
            file_path = os.path.join(EXCEL_PATH, file)
            logger.info(f"  📄 Traitement: {file}")
            
            try:
                df = pd.read_excel(file_path)
                
                for idx, row in df.iterrows():
                    # Nettoyer les NaN
                    record = row.to_dict()
                    record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                    
                    # Clé = client_code ou index
                    key = str(record.get('client_code', f'{file}_{idx}'))
                    
                    success = self.send_record(
                        topic=TOPICS['client'],
                        key=key,
                        value=record,
                        domain='client',
                        source_file=file
                    )
                    
                    if success:
                        total_records += 1
                
                logger.info(f"  ✅ {len(df)} enregistrements envoyés depuis {file}")
                
            except Exception as e:
                logger.error(f"  ❌ Erreur avec {file}: {e}")
        
        self.producer.flush()
        logger.info(f"✅ CLIENT: {total_records} enregistrements totaux ingérés\n")
        return total_records
    
    def ingest_product_files(self):
        """Ingère les fichiers produits vers Kafka"""
        logger.info("📂 Ingestion des fichiers PRODUCT...")
        
        files = [f for f in os.listdir(EXCEL_PATH) if f.startswith('PRODUCT_SOURCE_')]
        total_records = 0
        
        for file in files:
            file_path = os.path.join(EXCEL_PATH, file)
            logger.info(f"  📄 Traitement: {file}")
            
            try:
                df = pd.read_excel(file_path)
                
                for idx, row in df.iterrows():
                    record = row.to_dict()
                    record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                    
                    key = str(record.get('product_code', record.get('ean', f'{file}_{idx}')))
                    
                    success = self.send_record(
                        topic=TOPICS['product'],
                        key=key,
                        value=record,
                        domain='product',
                        source_file=file
                    )
                    
                    if success:
                        total_records += 1
                
                logger.info(f"  ✅ {len(df)} enregistrements envoyés depuis {file}")
                
            except Exception as e:
                logger.error(f"  ❌ Erreur avec {file}: {e}")
        
        self.producer.flush()
        logger.info(f"✅ PRODUCT: {total_records} enregistrements totaux ingérés\n")
        return total_records
    
    def ingest_supplier_files(self):
        """Ingère les fichiers fournisseurs vers Kafka"""
        logger.info("📂 Ingestion des fichiers SUPPLIER...")
        
        files = [f for f in os.listdir(EXCEL_PATH) if f.startswith('SUPPLIER_SOURCE_')]
        total_records = 0
        
        for file in files:
            file_path = os.path.join(EXCEL_PATH, file)
            logger.info(f"  📄 Traitement: {file}")
            
            try:
                df = pd.read_excel(file_path)
                
                for idx, row in df.iterrows():
                    record = row.to_dict()
                    record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                    
                    key = str(record.get('supplier_code', f'{file}_{idx}'))
                    
                    success = self.send_record(
                        topic=TOPICS['supplier'],
                        key=key,
                        value=record,
                        domain='supplier',
                        source_file=file
                    )
                    
                    if success:
                        total_records += 1
                
                logger.info(f"  ✅ {len(df)} enregistrements envoyés depuis {file}")
                
            except Exception as e:
                logger.error(f"  ❌ Erreur avec {file}: {e}")
        
        self.producer.flush()
        logger.info(f"✅ SUPPLIER: {total_records} enregistrements totaux ingérés\n")
        return total_records
    
    def ingest_all(self):
        """Ingère tous les fichiers"""
        logger.info("="*60)
        logger.info("🚀 DÉMARRAGE DE L'INGESTION KAFKA")
        logger.info("="*60 + "\n")
        
        start_time = time.time()
        
        # Ingestion par domaine
        client_count = self.ingest_client_files()
        product_count = self.ingest_product_files()
        supplier_count = self.ingest_supplier_files()
        
        total_time = time.time() - start_time
        
        logger.info("="*60)
        logger.info("📊 RÉSUMÉ DE L'INGESTION")
        logger.info("="*60)
        logger.info(f"CLIENT    : {client_count} enregistrements")
        logger.info(f"PRODUCT   : {product_count} enregistrements")
        logger.info(f"SUPPLIER  : {supplier_count} enregistrements")
        logger.info(f"TOTAL     : {client_count + product_count + supplier_count} enregistrements")
        logger.info(f"DURÉE     : {total_time:.2f} secondes")
        logger.info("="*60)
    
    def close(self):
        """Ferme le producer"""
        self.producer.close()
        logger.info("🔌 Producer Kafka fermé")


def main():
    """Fonction principale"""
    
    # Vérifier que le dossier Excel existe
    if not os.path.exists(EXCEL_PATH):
        logger.error(f"❌ Dossier {EXCEL_PATH} introuvable!")
        logger.info(f"Créez le dossier et ajoutez vos fichiers Excel")
        return
    
    # Vérifier qu'il y a des fichiers
    files = [f for f in os.listdir(EXCEL_PATH) if f.endswith('.xlsx')]
    if not files:
        logger.error(f"❌ Aucun fichier Excel trouvé dans {EXCEL_PATH}")
        return
    
    logger.info(f"📁 {len(files)} fichiers Excel trouvés")
    
    # Créer le producer
    producer = MDMKafkaProducer(KAFKA_BOOTSTRAP_SERVERS)
    
    try:
        # Lancer l'ingestion
        producer.ingest_all()
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interruption utilisateur")
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()