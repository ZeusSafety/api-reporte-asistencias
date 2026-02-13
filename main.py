import functions_framework
import pymysql
import json
import requests
import os
import io
import logging
from datetime import datetime
from google.cloud import storage
import requests


# Conexión a MySQL
def get_connection():
    conn = pymysql.connect(
        user="zeussafety-2024",
        password="ZeusSafety2025",
        db="Zeus_Safety_Data_Integration",
        unix_socket="/cloudsql/stable-smithy-435414-m6:us-central1:zeussafety-2024",
        cursorclass=pymysql.cursors.DictCursor
    )

    # Para establecer la zona horaria a UTC-5
    with conn.cursor() as cursor:
        cursor.execute("SET time_zone = '-05:00'")
    return conn


## Función de Subida a Cloud Storage
# Variables globales para el cliente y el bucket de GCS
storage_client = storage.Client()
BUCKET_NAME = "archivos_sistema"
GCS_FOLDER = "incidencias_areas_zeus"

def upload_to_gcs(file):
    """Sube el archivo PDF recibido del Front-end a GCS"""
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        object_name = f"{GCS_FOLDER}/{file.filename}"
        blob = bucket.blob(object_name)
        blob.upload_from_file(file, content_type=file.content_type)
        return f"https://storage.googleapis.com/{BUCKET_NAME}/{object_name}"
    except Exception as e:
        logging.error(f"Error en GCS: {e}")
        return None

# =================================================================
#                REGISTRO DE DATOS DE ASISTENCIA 
# =================================================================

def registrar_reporte_completo(request, conn, headers):
    """Procesa el FormData: PDF + Datos de Asistencia"""
    try:
        # 1. Extraer archivos y datos del FormData
        if 'file' not in request.files:
            return (json.dumps({"error": "Falta el archivo PDF"}), 400, headers)
        
        pdf_file = request.files['file']
        registrado_por = request.form.get("registrado_por")
        area = request.form.get("area")
        periodo = request.form.get("periodo")
        # El front debe enviar el array de registros como string JSON
        asistencias_json = json.loads(request.form.get("asistencias", "[]"))

        # 2. Subir PDF a Storage
        url_pdf = upload_to_gcs(pdf_file)
        if not url_pdf:
            return (json.dumps({"error": "Error al guardar PDF en la nube"}), 500, headers)

        # 3. Guardar en Base de Datos
        with conn.cursor() as cursor:
            # Insertar Bitácora
            sql_carga = "INSERT INTO registros_carga (periodo, registrado_por, area, pdf_reporte) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql_carga, (periodo, registrado_por, area, url_pdf))
            id_registro = cursor.lastrowid

            # Insertar Empleados y Asistencias
            for reg in asistencias_json:
                cursor.execute("INSERT IGNORE INTO empleados (id_empleado, nombre) VALUES (%s, %s)", 
                             (reg['id'], reg['nombre']))

                sql_asist = """INSERT INTO asistencias (id_empleado, id_registro, fecha, hora_entrada, hora_salida) 
                               VALUES (%s, %s, %s, %s, %s)"""
                cursor.execute(sql_asist, (
                    reg['id'], id_registro, reg['fecha'],
                    reg.get('entrada') or None, 
                    reg.get('salida') or None
                ))
            
            conn.commit()
            return (json.dumps({"success": True, "message": "Datos cargados correctamente", "url": url_pdf, "id_registro": id_registro}), 200, headers)

    except Exception as e:
        if conn: conn.rollback()
        logging.error(f"Error en registro: {e}")
        return (json.dumps({"error": str(e)}), 500, headers)

# =================================================================
#          OBTENER HISTORIAL DE CARGAS (REGISTROS_CARGA)
# =================================================================
def obtener_historial_cargas(conn, headers):
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT id_registro, periodo, registrado_por, area, pdf_reporte, created_at
                FROM registros_carga
                ORDER BY id_registro DESC
            """
            cursor.execute(sql)
            resultados = cursor.fetchall()
            
            historial = []
            for r in resultados:
                # Manejar tanto diccionarios como tuplas
                if isinstance(r, dict):
                    historial.append({
                        'id_registro': r['id_registro'],
                        'periodo': r['periodo'],
                        'registrado_por': r['registrado_por'],
                        'area': r['area'],
                        'pdf_reporte': r['pdf_reporte'],
                        'created_at': str(r.get('created_at', ''))
                    })
                else:
                    historial.append({
                        'id_registro': r[0],
                        'periodo': r[1],
                        'registrado_por': r[2],
                        'area': r[3],
                        'pdf_reporte': r[4],
                        'created_at': str(r[5]) if len(r) > 5 else ''
                    })
            
            return (json.dumps(historial), 200, headers)
    except Exception as e:
        logging.error(f"Error al obtener historial: {e}")
        return (json.dumps({"error": str(e)}), 500, headers)

# =================================================================
#                DE DATOS DE ASISTENCIA PARA DASHBOARD
# =================================================================
def obtener_datos_dashboard(conn, headers):
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT a.*, e.nombre, YEAR(a.fecha) as anio, MONTH(a.fecha) as mes
                FROM asistencias a
                JOIN empleados e ON a.id_empleado = e.id_empleado
                ORDER BY a.fecha DESC
            """
            cursor.execute(sql)
            resultados = cursor.fetchall()
            
            for r in resultados:
                r['fecha'] = r['fecha'].strftime('%Y-%m-%d')
                if r['hora_entrada']: r['hora_entrada'] = str(r['hora_entrada'])
                if r['hora_salida']: r['hora_salida'] = str(r['hora_salida'])

            return (json.dumps(resultados), 200, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)

# =================================================================
#                       FUNCIÓN PRINCIPAL (ROUTER)
# =================================================================

API_TOKEN = "https://api-verificacion-token-2946605267.us-central1.run.app"

# Función HTTP principal
@functions_framework.http
def reporteAsistencias(request):
    
    # 1. Definición de headers local (se usa en todos los retornos)
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Content-Type': 'application/json'
    }

    try:
        # Obtener el token del header Authorization
        auth_header = request.headers.get("Authorization")
        
        # Log para debugging
        logging.info(f"Authorization header recibido: {auth_header[:50] if auth_header else 'None'}...")
        
        # Validar que el token exista
        if not auth_header:
            return (json.dumps({"error": "Token no proporcionado"}), 401, headers)
        
        # Preparar headers para la verificación del token
        token_headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header
        }
        
        # Log para debugging
        logging.info(f"Verificando token en: {API_TOKEN}")
        logging.info(f"Headers enviados: Authorization={auth_header[:50]}...")
        
        # Verificar el token con la API de autenticación
        try:
            # Enviar POST sin body (solo headers)
            response = requests.post(API_TOKEN, headers=token_headers, timeout=10)
            
            # Log para debugging
            logging.info(f"Respuesta de token API: status={response.status_code}, body={response.text[:200]}")
            
            if response.status_code != 200:
                # transformamos json a diccionarios
                error_response = response.json()
                if "error" in error_response:
                    error_msg = error_response["error"]
                logging.warning(f"Token no autorizado: {error_msg}")
                return (json.dumps({"error": error_msg}), 401, headers)
        except requests.exceptions.RequestException as e:
            # Error de conexión o timeout
            logging.error(f"Error al verificar token: {str(e)}")
            return (json.dumps({"error": f"Error al verificar token: {str(e)}"}), 503, headers)
    except Exception as e:
        return (json.dumps({"error": str(e)}), 500, headers)

    # Manejo de OPTIONS (CORS preflight)
    if request.method == "OPTIONS":
        return ("", 204, headers)
    
    # Enrutamiento (Routing)
    path = request.path
    method = request.method
    conn = get_connection()

    try:
       if method == 'POST' and (path.endswith('/guardar-reporte') or path == '/'):
            return registrar_reporte_completo(request, conn, headers)
       
       elif method == 'GET' and path.endswith('/historial-cargas'):
            return obtener_historial_cargas(conn, headers)
        
       elif method == 'GET' and (path.endswith('/dashboard') or path == '/'):
            return obtener_datos_dashboard(conn, headers)
        
       else:
            return (json.dumps({'error': 'Ruta no encontrada'}), 404, headers)
    
    except Exception as e:
        # Captura errores de servidor o de lógica de negocio (Errores 500)
        return (json.dumps({'success': False, 'error': f'Error interno del servidor: {str(e)}'}), 500, headers)
    
    