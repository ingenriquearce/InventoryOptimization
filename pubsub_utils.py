import os
import streamlit as st
from google.oauth2 import service_account
from google.cloud import pubsub_v1, bigquery
import json
import random

# 📌 **Autenticación**
# Ruta al archivo JSON de las credenciales de servicio
#key_path = "C:\\Users\\Enrique\\Documents\\Proyectos\\inventoryoptimization-455112-946c0b30b97c.json"

# Configura las credenciales para Google Cloud
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Verificar la autenticación
#credentials = service_account.Credentials.from_service_account_file(key_path)
#print("Autenticado correctamente como:", credentials.service_account_email)


# Cargar credenciales desde Streamlit Secrets
service_account_info = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
credentials = service_account.Credentials.from_service_account_info(service_account_info)

print("Autenticado correctamente como:", credentials.service_account_email)

# Configuración
PROJECT_ID = "inventoryoptimization-455112"
TOPIC_ID = "sales-topic"
DATASET_ID = "optimization_Inventory"
SALES_TABLE = "fact_sales"
PRODUCTS_TABLE = "dim_products"

# Inicializar clientes
publisher = pubsub_v1.PublisherClient()
bq_client = bigquery.Client()

def obtener_sale_id():
    """Genera un sale_id único basado en BigQuery."""
    query = f"SELECT sale_id FROM `{PROJECT_ID}.{DATASET_ID}.{SALES_TABLE}`"
    existing_sales = {row.sale_id for row in bq_client.query(query)}

    while True:
        sale_id = random.randint(100000, 999999)
        if sale_id not in existing_sales:
            return sale_id

def obtener_precio_producto(product_id):
    """Obtiene el precio de un producto desde BigQuery."""
    query_price = f"""
    SELECT price FROM `{PROJECT_ID}.{DATASET_ID}.{PRODUCTS_TABLE}`
    WHERE product_id = '{product_id}'
    """
    price_result = bq_client.query(query_price).result()
    price = next(iter(price_result), None)

    return price.price if price else None

def enviar_venta_pubsub(store_id, product_id, customer_id, quantity, sale_date):
    """Publica un mensaje en Pub/Sub con los datos de la venta."""
    sale_id = obtener_sale_id()
    price = obtener_precio_producto(product_id)

    if price is None:
        raise ValueError(f"❌ No se encontró precio para el producto {product_id}")

    total_sale_value = quantity * price

    sale_event = {
        "sale_id": sale_id,
        "store_id": store_id,
        "product_id": product_id,
        "customer_id": customer_id,
        "quantity": quantity,
        "sale_date": sale_date,
        "total_sale_value": total_sale_value
    }

    message_data = json.dumps(sale_event).encode("utf-8")
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    future = publisher.publish(topic_path, message_data)
    message_id = future.result()

    return message_id, sale_event

