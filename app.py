"""
app.py - DataCleanse Pro · Enterprise
Versión híbrida: crea carpetas locales + descarga directa de archivos
"""

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file
import pandas as pd
import os, re, glob, traceback, subprocess, platform, io
from datetime import datetime
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = "datacleanse-secret-2024"

# ============================================================
# USUARIOS INVITADOS
# ============================================================
USUARIOS = {
    "admin":    generate_password_hash("admin123"),
    "usuario1": generate_password_hash("clave123"),
    "usuario2": generate_password_hash("clave456"),
    "luis.vargas@saviasaludeps.com": generate_password_hash("Laurenv*2018."),
}

# ============================================================
# CONFIGURACION
# ============================================================
COLUMNA_FACTURA = "numero_facturado"
COLUMNA_FECHA   = "fecha_prestacion"
EXTENSIONES     = ["*.csv", "*.txt", "*.xlsx", "*.xls", "*.xlsm"]

VALORES_SIN_CONTRATO = {'SIN CONTRATO', 'SINCONTRATO', 'NA', 'N/A', 'VARIOS', '0', 'NONE', '', 'nan', 'None'}

# Carpeta base: detecta si es Railway, Vercel o Render
ES_RAILWAY = os.environ.get("RAILWAY_ENVIRONMENT") is not None
ES_VERCEL  = os.environ.get("VERCEL") is not None
ES_RENDER  = os.environ.get("RENDER") is not None
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))

# Estructura de datos
ESTRUCTURA_FINAL = [
    "numero_radicado", "nit", "ips", "numero_contrato",
    "numero_facturado", "valor_factura", "valor_inicial_glosa",
    "valor_pendiente", "valor_pagado_factura", "valor_copago",
    "valor_aceptado_ips", "valor_pagado_eps", "mae_tipo_contrato_valor",
    "fecha_radicacion", "fecha_proceso_radicacion",
    "fecha_prestacion", "estado_factura", "mae_regimen_valor"
]

MAPEO_COLUMNAS = {
    "valor_pendiente_actual":  "valor_pendiente",
    "valor_aceptado_eps":      "valor_pagado_eps",
    "fecha_de_prestacion":      "fecha_prestacion",
    "fecha_de_radicacion":      "fecha_radicacion",
    "numero_de_factura":        "numero_facturado",
    "numero_de_contrato":       "numero_contrato",
    "fecha_prestacion_servicio": "fecha_prestacion"
}

COLUMNAS_IGNORAR = {"naturaleza_juridica"}

def aplicar_filtro_regimen(df, regimen):
    if regimen != "TODOS" and "mae_regimen_valor" in df.columns:
        return df[df["mae_regimen_valor"].astype(str).str.strip().str.upper() == regimen]
    return df

def aplicar_filtro_fechas(df, fecha_inicio, fecha_fin, carpeta=None):
    if not fecha_inicio and not fecha_fin:
        return df
    col = None
    posibles = ["fecha_prestacion", "fecha_de_prestacion", "fecha_prestacion_servicio", "fecha_prestacion_del_servicio"]
    
    # 1. Buscar coincidencia exacta
    for c in posibles:
        if c in df.columns:
            col = c
            break
            
    # 2. Buscar por coincidencia parcial si no se encontró
    if not col:
        for c in df.columns:
            cstr = str(c).lower()
            if 'fecha' in cstr and 'prestacion' in cstr:
                col = c
                break
                
    if not col:
        return df
        
    try:
        # Convertir a datetime la columna
        fechas_dt = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
        # Por si el formato es americano
        fechas_dt = fechas_dt.fillna(pd.to_datetime(df[col], dayfirst=False, errors="coerce"))
        
        mask = pd.Series(True, index=df.index)
        
        if fecha_inicio:
            dt_inicio = pd.to_datetime(fecha_inicio)
            mask = mask & (fechas_dt >= dt_inicio)
            
        if fecha_fin:
            # Añadimos 23:59:59 al final del día
            dt_fin = pd.to_datetime(fecha_fin) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            mask = mask & (fechas_dt <= dt_fin)
            
        # IMPORTANTE: Conservar las facturas que tengan la celda de fecha vacía o no reconocible
        mask_final = mask | fechas_dt.isna()
        
        df_descartado = df[~mask_final].copy()
        if carpeta and not df_descartado.empty:
            try:
                import os
                # Guardar en la carpeta de Duplicados para que se descargue en el ZIP
                ruta_desc = os.path.join(carpeta, "Duplicados", "Descartados_por_fecha.xlsx")
                os.makedirs(os.path.dirname(ruta_desc), exist_ok=True)
                if os.path.exists(ruta_desc):
                    df_prev = pd.read_excel(ruta_desc)
                    df_descartado = pd.concat([df_prev, df_descartado], ignore_index=True)
                df_descartado.to_excel(ruta_desc, index=False)
            except:
                pass
        
        return df[mask_final].copy()
    except Exception as e:
        print("Error filtrando fechas:", e)
        return df

def normalizar_columnas(df):
    df = df.rename(columns=MAPEO_COLUMNAS)
    cols_ignorar = [c for c in df.columns if c in COLUMNAS_IGNORAR]
    if cols_ignorar:
        df = df.drop(columns=cols_ignorar)
    for col in ESTRUCTURA_FINAL:
        if col not in df.columns:
            df[col] = None
    return df[ESTRUCTURA_FINAL]

def unificar_archivos(rutas):
    frames = []
    for ruta in rutas:
        df = leer_archivo(ruta)
        df = limpiar_nombres_columnas(df)
        df = normalizar_columnas(df)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)

def carpeta_usuario(username):
    """
    Define la ruta de almacenamiento según el entorno:
    - Vercel/Render: /tmp/{username}
    - Railway: {BASE_DIR}/user_data/{username}
    - Local: {BASE_DIR}/{username}
    """
    if ES_VERCEL or ES_RENDER:
        path = os.path.join("/tmp", username)
    elif ES_RAILWAY:
        path = os.path.join(BASE_DIR, "user_data", username)
    else:
        path = os.path.join(BASE_DIR, username) if username != "admin" else BASE_DIR
    
    os.makedirs(path, exist_ok=True)
    return path

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "usuario" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

def extraer_contrato(nombre_archivo):
    numeros = re.findall(r'\d{7,}', nombre_archivo)
    return numeros[0] if numeros else os.path.splitext(nombre_archivo)[0]

def limpiar_nombres_columnas(df):
    # Limpieza profunda: minúsculas, sin espacios extra, reemplazar espacios por guiones bajos
    df.columns = df.columns.astype(str).str.strip().str.lower() \
                           .str.replace(' ', '_', regex=False) \
                           .str.replace('.', '', regex=False) \
                           .str.replace('\ufeff', '', regex=False)
    return df

def leer_archivo(ruta):
    _, ext = os.path.splitext(ruta)
    ext = ext.lower()
    if ext in [".xlsx", ".xls", ".xlsm"]:
        return pd.read_excel(ruta)
    encodings = ["utf-8-sig", "latin-1", "iso-8859-1", "cp1252"]
    for encoding in encodings:
        try:
            df_prueba = pd.read_csv(ruta, nrows=5, header=None, encoding=encoding)
            if df_prueba.shape[1] == 1:
                primera_linea = str(df_prueba.iloc[0, 0]) if len(df_prueba) > 0 else ""
                if '|' in primera_linea:
                    df = pd.read_csv(ruta, sep='|', encoding=encoding, low_memory=False)
                elif ';' in primera_linea:
                    df = pd.read_csv(ruta, sep=';', encoding=encoding, low_memory=False)
                else:
                    df = pd.read_csv(ruta, sep=',', encoding=encoding, low_memory=False)
            else:
                df = pd.read_csv(ruta, sep=None, engine="python", encoding=encoding)
            df.columns = df.columns.str.strip()
            for col in df.columns:
                if df[col].dtype == object:
                    muestra = df[col].dropna().head(10).astype(str)
                    if muestra.str.contains(r'\$').any():
                        df[col] = (df[col].astype(str).str.strip().str.replace('$', '', regex=False).str.strip().str.replace('.', '', regex=False).str.replace(',', '.', regex=False).replace('nan', None))
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            return df
        except: continue
    return pd.read_csv(ruta, sep=None, engine="python", encoding="latin-1")

def guardar_excel(df, ruta, nombre_hoja):
    nombre_hoja = nombre_hoja[:31]
    with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=nombre_hoja, index=False)
        ws = writer.sheets[nombre_hoja]
        for col in ws.columns:
            valores = [str(c.value) if c.value is not None else "" for c in col[:6]]
            ancho = min(max((len(v) for v in valores), default=10) + 4, 45)
            ws.column_dimensions[col[0].column_letter].width = ancho

def separar_duplicados(df):
    # 1. Normalización de Factura (más agresiva)
    def clean_id(val):
        if pd.isna(val): return "NAN"
        s = str(val).strip().upper()
        # Eliminar ceros a la izquierda para que '00123' y '123' sean iguales
        s = s.lstrip('0')
        # Eliminar .0 al final (común en Excel)
        if s.endswith('.0'): s = s[:-2]
        # Si después de limpiar queda vacío, era '0' o similar
        if not s: return "0"
        return s
    
    df[COLUMNA_FACTURA] = df[COLUMNA_FACTURA].apply(clean_id)
    
    # 2. Búsqueda de la fecha más reciente (VIGENTE) en toda la fila
    def parse_fecha_inteligente(row):
        fechas_encontradas = []
        # Columnas preferentes
        prioridad = [COLUMNA_FECHA, "fecha_radicacion", "fecha_proceso_radicacion"]
        # Otras posibles columnas de fecha
        otras_cols = [c for c in row.index if 'fecha' in str(c).lower() and c not in prioridad]
        
        for col in (prioridad + otras_cols):
            val = row.get(col)
            if pd.notnull(val) and str(val).strip() != "":
                try:
                    # Intentar parsear con día primero (estándar ES)
                    dt = pd.to_datetime(val, dayfirst=True, errors='coerce')
                    if pd.isna(dt):
                        # Intentar formato flexible si el anterior falló
                        dt = pd.to_datetime(val, dayfirst=False, errors='coerce')
                    
                    if pd.notnull(dt):
                        fechas_encontradas.append(dt)
                except: continue
        
        if fechas_encontradas:
            # IMPORTANTE: Devolvemos la fecha más RECIENTE encontrada en esta fila
            return max(fechas_encontradas)
        return pd.NaT

    df["_dt_final"] = df.apply(parse_fecha_inteligente, axis=1)
    df["_ts"] = df["_dt_final"].apply(lambda x: x.timestamp() if pd.notnull(x) else -1.0)
    
    # 3. PRIORIDAD DE CONTRATO (NUEVO)
    # Priorizar filas que tengan un número de contrato válido sobre las que dicen "SIN CONTRATO", "0", etc.
    def tiene_contrato_valido(val):
        if pd.isna(val): return 0
        s = str(val).strip().upper()
        if s in VALORES_SIN_CONTRATO: return 0
        # Si tiene al menos un carácter alfanumérico y no está en la lista de descartes
        return 1 if any(c.isalnum() for c in s) else 0

    df["_has_contract"] = df["numero_contrato"].apply(tiene_contrato_valido)

    # 4. Score de Completitud (priorizar filas con más datos útiles)
    cols_datos = [c for c in ESTRUCTURA_FINAL if c not in [COLUMNA_FACTURA, COLUMNA_FECHA]]
    def score(val):
        if pd.isna(val): return 0
        s = str(val).strip().upper()
        if s in ["0", "0.0", "", "NONE", "NAN", "N/A", "NA", "0,0", "NULL"] or s in VALORES_SIN_CONTRATO: return 0
        return 1
    
    df["_comp"] = 0
    for col in cols_datos:
        if col in df.columns:
            df["_comp"] += df[col].map(score)
            
    # 5. ORDENAMIENTO CRÍTICO:
    # 1. Factura
    # 2. ¿Tiene contrato? (1 primero, 0 después)
    # 3. Fecha más reciente (desc)
    # 4. Completitud (desc)
    df_ord = df.sort_values(
        by=[COLUMNA_FACTURA, "_has_contract", "_ts", "_comp"], 
        ascending=[True, False, False, False]
    )
    
    # 6. Mantener el primero (el mejor postor)
    df_limpio     = df_ord.drop_duplicates(subset=[COLUMNA_FACTURA], keep="first").copy()
    df_duplicados = df_ord[~df_ord.index.isin(df_limpio.index)].copy()
    
    # Limpieza de columnas auxiliares
    cols_aux = ["_dt_final", "_ts", "_has_contract", "_comp"]
    for frame in [df_limpio, df_duplicados]:
        cols_existentes = [c for c in cols_aux if c in frame.columns]
        if cols_existentes:
            frame.drop(columns=cols_existentes, inplace=True)
        
    return df_limpio.reset_index(drop=True), df_duplicados.reset_index(drop=True)

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("usuario", "").strip()
        pwd  = request.form.get("clave", "")
        if user in USUARIOS and check_password_hash(USUARIOS[user], pwd):
            session["usuario"] = user
            return redirect(url_for("index"))
        error = "Usuario o contraseña incorrectos"
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
@login_required
def index():
    return render_template("index.html", usuario=session["usuario"])

@app.route("/api/listar", methods=["POST"])
@login_required
def listar_archivos():
    carpeta = carpeta_usuario(session["usuario"])
    archivos = []
    for ext in EXTENSIONES:
        archivos.extend(glob.glob(os.path.join(carpeta, ext)))
    archivos = [a for a in archivos if not a.endswith(".py") and "Sin Duplicados" not in os.path.basename(a) and "Duplicados" not in os.path.basename(a)]
    archivos_info = [{"nombre": os.path.basename(a), "ruta": a} for a in sorted(archivos)]
    return jsonify({"archivos": archivos_info, "carpeta": carpeta})

@app.route("/api/subir", methods=["POST"])
@login_required
def subir_archivos():
    import zipfile
    try:
        import rarfile
    except ImportError:
        rarfile = None

    carpeta  = carpeta_usuario(session["usuario"])
    archivos = request.files.getlist("archivos")
    if not archivos: return jsonify({"error": "No se recibieron archivos"}), 400
    
    ext_validas = {".csv", ".txt", ".xlsx", ".xls", ".xlsm", ".zip", ".rar"}
    ext_datos = {".csv", ".txt", ".xlsx", ".xls", ".xlsm"}
    guardados = []
    
    for f in archivos:
        _, ext = os.path.splitext(f.filename)
        ext_lower = ext.lower()
        if ext_lower not in ext_validas: continue
        
        if ext_lower == '.zip':
            try:
                import io
                f.seek(0)
                with zipfile.ZipFile(f.stream, 'r') as z:
                    for file_name in z.namelist():
                        _, c_ext = os.path.splitext(file_name)
                        if c_ext.lower() in ext_datos:
                            base_name = os.path.basename(file_name)
                            if base_name:  # No es un directorio
                                dest_path = os.path.join(carpeta, base_name)
                                with z.open(file_name) as source, open(dest_path, 'wb') as target:
                                    target.write(source.read())
                                guardados.append(base_name)
            except Exception as e:
                return jsonify({"error": f"Error extrayendo ZIP: {str(e)}"}), 400
                
        elif ext_lower == '.rar':
            if rarfile:
                if platform.system() == "Windows":
                    posibles_rutas = [
                        r"C:\Program Files\WinRAR\UnRAR.exe",
                        r"C:\Program Files (x86)\WinRAR\UnRAR.exe"
                    ]
                    for ruta in posibles_rutas:
                        if os.path.exists(ruta):
                            rarfile.UNRAR_TOOL = ruta
                            break
                try:
                    with rarfile.RarFile(f, 'r') as r:
                        for file_name in r.namelist():
                            _, c_ext = os.path.splitext(file_name)
                            if c_ext.lower() in ext_datos:
                                base_name = os.path.basename(file_name)
                                if base_name:
                                    dest_path = os.path.join(carpeta, base_name)
                                    with r.open(file_name) as source, open(dest_path, 'wb') as target:
                                        target.write(source.read())
                                    guardados.append(base_name)
                except Exception as e:
                    return jsonify({"error": f"Error extrayendo RAR: {str(e)}\nProbablemente WinRAR no está en C:\\Program Files\\WinRAR"}), 400
            else:
                return jsonify({"error": "La libreria rarfile no está instalada (ejecuta pip install rarfile)"}), 400
        else:
            f.save(os.path.join(carpeta, f.filename))
            guardados.append(f.filename)
            
    if not guardados:
        return jsonify({"error": "El archivo comprimido estaba vacío o no contenía Excels/CSVs"}), 400

    return jsonify({"ok": True, "guardados": guardados, "total": len(guardados)})

@app.route("/api/procesar", methods=["POST"])
@login_required
def procesar():
    carpeta  = carpeta_usuario(session["usuario"])
    data     = request.json
    archivos = data.get("archivos", [])
    carpeta_limpios = os.path.join(carpeta, "Sin Duplicados")
    carpeta_duplicados = os.path.join(carpeta, "Duplicados")
    os.makedirs(carpeta_limpios, exist_ok=True)
    os.makedirs(carpeta_duplicados, exist_ok=True)
    for f in glob.glob(os.path.join(carpeta_limpios, "*.xlsx")): os.remove(f)
    for root, dirs, files in os.walk(carpeta_duplicados):
        for f in files: os.remove(os.path.join(root, f))
    resultados = []
    for ruta_archivo in archivos:
        nombre_archivo = os.path.basename(ruta_archivo)
        nombre_base    = os.path.splitext(nombre_archivo)[0]
        contrato       = extraer_contrato(nombre_archivo)
        try:
            df = leer_archivo(ruta_archivo)
            df = limpiar_nombres_columnas(df)
            df = aplicar_filtro_regimen(df, data.get("regimen", "TODOS"))
            df = aplicar_filtro_fechas(df, data.get("fecha_inicio"), data.get("fecha_fin"), carpeta)
            filas_orig = len(df)
            if COLUMNA_FACTURA not in df.columns or COLUMNA_FECHA not in df.columns:
                resultados.append({"archivo": nombre_archivo, "estado": "error", "mensaje": "Faltan columnas"})
                continue
            df_limpio, df_duplicados = separar_duplicados(df)
            ruta_limpio = os.path.join(carpeta_limpios, f"{nombre_base}.xlsx")
            guardar_excel(df_limpio, ruta_limpio, contrato)
            ruta_dup = None
            nombre_dup = None
            if len(df_duplicados) > 0:
                carpeta_contrato = os.path.join(carpeta_duplicados, contrato)
                os.makedirs(carpeta_contrato, exist_ok=True)
                nombre_dup = f"{nombre_base}_duplicados.xlsx"
                ruta_dup   = os.path.join(carpeta_contrato, nombre_dup)
                guardar_excel(df_duplicados, ruta_dup, f"{contrato}_dup")
            os.remove(ruta_archivo)
            resultados.append({"archivo": nombre_archivo, "estado": "ok", "contrato": contrato, "filas_originales": filas_orig, "duplicados_eliminados": len(df_duplicados), "filas_resultado": len(df_limpio)})
        except Exception as e:
            resultados.append({"archivo": nombre_archivo, "estado": "error", "mensaje": str(e)})
    return jsonify({"resultados": resultados})

@app.route("/api/unificar", methods=["POST"])
@login_required
def unificar():
    carpeta  = carpeta_usuario(session["usuario"])
    data     = request.json
    archivos = data.get("archivos", [])
    if len(archivos) < 2: return jsonify({"error": "Selecciona al menos 2 archivos"}), 400
    carpeta_limpios = os.path.join(carpeta, "Sin Duplicados")
    carpeta_duplicados = os.path.join(carpeta, "Duplicados")
    os.makedirs(carpeta_limpios, exist_ok=True)
    os.makedirs(carpeta_duplicados, exist_ok=True)
    
    # Limpiar carpetas antes de unificar para evitar archivos acumulados en el zip
    for f in glob.glob(os.path.join(carpeta_limpios, "*.xlsx")): 
        try: os.remove(f)
        except: pass
    for root, dirs, files in os.walk(carpeta_duplicados):
        for f in files: 
            try: os.remove(os.path.join(root, f))
            except: pass

    try:
        df_unificado = unificar_archivos(archivos)
        df_unificado = aplicar_filtro_regimen(df_unificado, data.get("regimen", "TODOS"))
        df_unificado = aplicar_filtro_fechas(df_unificado, data.get("fecha_inicio"), data.get("fecha_fin"), carpeta)
        filas_orig   = len(df_unificado)
        df_limpio, df_duplicados = separar_duplicados(df_unificado)
        nombre_base = f"unificado_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        ruta_limpio = os.path.join(carpeta_limpios, f"{nombre_base}.xlsx")
        guardar_excel(df_limpio, ruta_limpio, "Unificado")
        ruta_dup = None
        if len(df_duplicados) > 0:
            carpeta_dup = os.path.join(carpeta_duplicados, "unificado")
            os.makedirs(carpeta_dup, exist_ok=True)
            ruta_dup = os.path.join(carpeta_dup, f"{nombre_base}_duplicados.xlsx")
            guardar_excel(df_duplicados, ruta_dup, "Duplicados")
        for ruta in archivos:
            if os.path.isfile(ruta): os.remove(ruta)
        return jsonify({
            "estado": "ok", 
            "version": "1.3_contract_priority",
            "archivos_unificados": len(archivos), 
            "filas_totales": filas_orig, 
            "duplicados_eliminados": len(df_duplicados), 
            "filas_resultado": len(df_limpio), 
            "nombre_limpio": f"{nombre_base}.xlsx"
        })
    except Exception as e:
        import gc
        gc.collect()
        return jsonify({"error": str(e)}), 500

@app.route("/api/descargar", methods=["POST"])
@login_required
def descargar():
    import zipfile
    data       = request.json
    tipo       = data.get("tipo", "limpios")
    carpeta    = carpeta_usuario(session["usuario"])
    subcarpeta = os.path.join(carpeta, "Sin Duplicados" if tipo == "limpios" else "Duplicados")
    if not os.path.isdir(subcarpeta): return jsonify({"error": "No hay archivos"}), 404
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(subcarpeta):
            for fname in files:
                full = os.path.join(root, fname)
                zf.write(full, os.path.relpath(full, subcarpeta))
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name=f"{tipo}.zip", mimetype="application/zip")

@app.route("/api/eliminar", methods=["POST"])
@login_required
def eliminar_archivo():
    data    = request.json
    archivo = data.get("archivo", "").strip()
    carpeta = carpeta_usuario(session["usuario"])
    ruta    = os.path.join(carpeta, archivo)
    if not os.path.abspath(ruta).startswith(os.path.abspath(carpeta)): return jsonify({"error": "No permitido"}), 403
    if os.path.isfile(ruta): os.remove(ruta)
    return jsonify({"ok": True})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(debug=False, host="0.0.0.0", port=port)
