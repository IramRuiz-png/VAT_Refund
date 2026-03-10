import os
import time
import streamlit as st
from dotenv import load_dotenv, find_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv(find_dotenv(), override=True)

# ---------- Helpers generales ----------

def _clean_env_int(var_name: str):
    raw = os.getenv(var_name)
    if raw is None:
        return None, None
    cleaned = raw.strip().strip('"').strip("'")
    try:
        return int(cleaned), cleaned
    except ValueError:
        return None, cleaned

# ---------- Config ----------

APP_TITLE = os.getenv("APP_TITLE", "VAT Refund – Lector/Entrenador")
CATALOG = os.getenv("CATALOG_NAME", "vat_refund")
SCHEMA = os.getenv("SCHEMA_NAME", "default")
DOCS = os.getenv("DOCUMENTS_FOLDER", "documents")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CONFIG_PROFILE = os.getenv("DATABRICKS_CONFIG_PROFILE")

JOB_ID_INT, JOB_ID_CLEAN = _clean_env_int("JOB_ID")

# ---------- Helpers de rutas ----------

def month_label(mes_es: str, year: int) -> str:
    return f"{mes_es} {year}"

def base_month_path(m_label: str) -> str:
    return f"/Volumes/{CATALOG}/{SCHEMA}/{DOCS}/{m_label.strip()}/"

def io_paths(m_label: str):
    base = base_month_path(m_label)
    input_dir = os.path.join(base, "Archivos de Entrada/")
    output_dir = os.path.join(base, "Archivos de Salida/") 
    return input_dir, output_dir


# Subida de archivos a Volumen Databricks
def upload_to_databricks_volume(local_file, dbx_path):
    w = _workspace_client()
    with open(local_file, "rb") as f:
        w.files.upload(dbx_path, f, overwrite=True)

def ensure_dirs(*paths):
    # Ya no se crean carpetas locales, solo se mantiene para compatibilidad
    pass

def _workspace_client():
    if DATABRICKS_HOST and DATABRICKS_TOKEN:
        return WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    if DATABRICKS_CONFIG_PROFILE:
        return WorkspaceClient()
    return WorkspaceClient()

# ---------- Jobs ----------

def run_job_and_wait(month_label: str):
    if JOB_ID_INT is None:
        raise RuntimeError("JOB_ID no configurado o inválido (.env).")

    try:
        w = _workspace_client()
        run = w.jobs.run_now(
            job_id=JOB_ID_INT,
            notebook_params={
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "documents_folder": DOCS,
                "month_run": month_label,
                "model_name": "vat_refund_agent",
            },
        )
        result = run.result()
        return result.run_id
    except Exception as e:
        raise RuntimeError(f"Error al autenticar o ejecutar el Job. Detalle: {e}")

# ---------- Streamlit UI ----------

st.set_page_config(page_title=APP_TITLE, page_icon="", layout="centered")

st.markdown("""
<style>
h1, h2, h3 { font-family: ui-sans-serif, system-ui; }
div.stDownloadButton > button, div.stButton > button {
  border-radius: 8px; padding: 0.6rem 1rem; font-weight: 600;
}
.block-container { padding-top: 2rem; }
</style>
""", unsafe_allow_html=True)

st.title(APP_TITLE)

with st.expander(" Config (debug)"):
    st.caption(f"cwd={os.getcwd()}")
    st.caption(
        "CATALOG={0} | SCHEMA={1} | DOCS={2} | JOB_ID={3}".format(
            CATALOG, SCHEMA, DOCS,
            ("OK" if JOB_ID_INT is not None else f"INVÁLIDO ({repr(JOB_ID_CLEAN)})")
        )
    )
    st.caption(
        "AUTH HOST={0} | TOKEN={1} | PROFILE={2}".format(
            DATABRICKS_HOST,
            ("***" + DATABRICKS_TOKEN[-4:]) if DATABRICKS_TOKEN else None,
            DATABRICKS_CONFIG_PROFILE,
        )
    )

tab1, tab2 = st.tabs([" Procesar", " Resultados"])

MESES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
         "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
YEAR_MIN, YEAR_MAX = 2024, 2032

# ---------- Tab Procesar ----------

with tab1:
    st.subheader("Sube archivos y ejecuta el proceso")

    colM, colY = st.columns([2,1])
    with colM:
        mes = st.selectbox("Mes", MESES, index=11)
    with colY:
        year = st.number_input("Año", min_value=YEAR_MIN, max_value=YEAR_MAX, value=2025, step=1)
    m_label = month_label(mes, year)

    files = st.file_uploader(
        "Archivos de entrada",
        accept_multiple_files=True,
        type=None,
        help="Puedes soltar varios archivos aquí."
    )

    run_btn = st.button("Subir y ejecutar", type="primary", use_container_width=True)
    
        
    if run_btn:
        if not files:
            st.error("Por favor, sube al menos un archivo.")
            st.stop()


        try:
            in_path, out_path = io_paths(m_label)
            # Guardar archivos temporales y subirlos a Databricks Volume
            import tempfile
            subidos = 0
            for f in files:
                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    tmp.write(f.getbuffer())
                    tmp.flush()
                    dbx_dest = os.path.join(in_path, f.name)
                    upload_to_databricks_volume(tmp.name, dbx_dest)
                    subidos += 1

            with st.status("Ejecutando notebook en Databricks…", expanded=True) as status:
                st.write(f" Input: `{in_path}`")
                st.write(f" Subidos {subidos} archivo(s) a Databricks Volume.")
                st.write(" Lanzando Job…")
                start = time.time()
                run_id = run_job_and_wait(m_label)
                elapsed = time.time() - start
                st.write(f" Job terminado. run_id = `{run_id}` en {elapsed:,.1f}s.")
                status.update(label="Proceso finalizado", state="complete")

            st.success("Listo. Ve a la pestaña Resultados para descargar los archivos de salida.")

        except Exception as e:
            st.error(f"Falló la ejecución: {e}")

# ---------- Tab Resultados ----------

with tab2:
    st.subheader("Descargar outputs")
    colM2, colY2 = st.columns([2,1])
    with colM2:
        mes2 = st.selectbox("Mes", MESES, index=11, key="m2")
    with colY2:
        year2 = st.number_input("Año", min_value=YEAR_MIN, max_value=YEAR_MAX, value=2025, step=1, key="y2")
    m_label2 = month_label(mes2, year2)
    _, out_path2 = io_paths(m_label2)

    if st.button("Listar outputs", use_container_width=True):
        try:
            w = _workspace_client()
            # Listar archivos en el volume de Databricks
            files_out = w.files.list(out_path2)
            files_out = [f for f in files_out if not f.is_dir]
            if not files_out:
                st.warning("No hay archivos en output/ todavía.")
            else:
                for f in files_out:
                    dbx_fp = os.path.join(out_path2, f.name)
                    # Descargar el archivo a memoria y ofrecerlo
                    file_bytes = w.files.download(dbx_fp).read()
                    st.download_button(
                        label=f"⬇Descargar {f.name}",
                        data=file_bytes,
                        file_name=f.name,
                        mime="application/octet-stream",
                        use_container_width=True
                    )
        except Exception as e:
            st.error(f"No se pudieron listar/descargar outputs: {e}")