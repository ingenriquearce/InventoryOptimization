import streamlit as st
from pubsub_utils import enviar_venta_pubsub
from google.cloud import ai
import os

# Configuración para el modelo Gemini (de Google AI)
key_path = "C:\\Users\\Enrique\\Documents\\Proyectos\\inventoryoptimization-455112-946c0b30b97c.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
client = ai.GeminiClient()

# Configura la página de Streamlit
st.title("Envío de Venta a Pub/Sub y Predicción con IA")
st.write("Este es el formulario para enviar una venta a Pub/Sub y almacenarla en BigQuery.")
st.write("O selecciona la opción de IA para hacer predicciones sobre demanda o productos.")

# Navegación entre pantallas
menu = st.radio("Selecciona una opción", ["Formulario de Venta", "IA - Predicción de Demanda"])

# Pantalla de Envío de Venta a Pub/Sub
if menu == "Formulario de Venta":
    st.subheader("Formulario para enviar una venta a Pub/Sub")

    # Formulario para ingresar los datos de la venta
    store_id = st.number_input("ID de la tienda", min_value=1)
    product_id = st.text_input("ID del producto")
    customer_id = st.number_input("ID del cliente", min_value=1)
    quantity = st.number_input("Cantidad de producto", min_value=1)
    sale_date = st.date_input("Fecha de la venta")

    # Botón para enviar la venta
    if st.button("Enviar Venta"):
        if store_id and product_id and customer_id and quantity and sale_date:
            try:
                # Enviar la venta a Pub/Sub
                message_id, sale_event = enviar_venta_pubsub(store_id, product_id, customer_id, quantity, str(sale_date))
                st.success(f"✅ Venta enviada con éxito! ID del mensaje: {message_id}")
                st.json(sale_event)
            except Exception as e:
                st.error(f"❌ Error al enviar la venta: {e}")
        else:
            st.warning("⚠️ Todos los campos son obligatorios.")
