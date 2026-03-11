import os
import time
import io
import re
import zipfile
import mimetypes
import posixpath
import streamlit as st
import json
from dotenv import load_dotenv, find_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

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
    return posixpath.join("/Volumes", CATALOG, SCHEMA, DOCS, m_label.strip())

def io_paths(m_label: str):
    base = base_month_path(m_label)
    input_dir = posixpath.join(base, "Archivos de Entrada")
    output_dir = posixpath.join(base, "Archivos de Salida")
    return input_dir, output_dir


def _entry_is_dir(entry) -> bool:
    is_directory = getattr(entry, "is_directory", None)
    if isinstance(is_directory, bool):
        return is_directory
    return (getattr(entry, "path", "") or "").endswith("/")


def _entry_name(entry) -> str:
    name = getattr(entry, "name", None)
    if name:
        return name
    return posixpath.basename((getattr(entry, "path", "") or "").rstrip("/"))


def _dedupe_entries(entries):
    unique = {}
    for entry in entries:
        path = (getattr(entry, "path", "") or "").rstrip("/")
        if not path:
            continue
        if path not in unique:
            unique[path] = entry
    return list(unique.values())


def _date_prefix(file_name: str):
    match = re.match(r"^(\d{4})_(\d{2})_(\d{2})_", file_name or "")
    if not match:
        return None
    year, month, day = match.groups()
    return int(year), int(month), int(day)


def _filter_latest_visible_entries(entries):
    visible_entries = [entry for entry in entries if not _entry_name(entry).lower().endswith(".txt")]
    dated_entries = []

    for entry in visible_entries:
        file_date = _date_prefix(_entry_name(entry))
        if file_date is not None:
            dated_entries.append((file_date, entry))

    if not dated_entries:
        return sorted(visible_entries, key=lambda entry: _entry_name(entry).lower()), None

    latest_date = max(item[0] for item in dated_entries)
    latest_entries = [entry for file_date, entry in dated_entries if file_date == latest_date]
    return sorted(latest_entries, key=lambda entry: _entry_name(entry).lower()), latest_date


def _download_file_bytes(w, file_path: str) -> bytes:
    with w.files.download(file_path).contents as stream:
        return stream.read()


def _list_files_recursive(w, base_dir: str):
    files = []
    seen_dirs = set()
    seen_files = set()
    pending = [base_dir.rstrip("/")]

    while pending:
        current_dir = pending.pop()
        if current_dir in seen_dirs:
            continue
        seen_dirs.add(current_dir)

        for entry in _dedupe_entries(list(w.files.list_directory_contents(current_dir))):
            entry_path = (getattr(entry, "path", "") or "").rstrip("/")
            if not entry_path:
                continue

            if _entry_is_dir(entry):
                pending.append(entry_path)
            elif entry_path not in seen_files:
                seen_files.add(entry_path)
                files.append(entry)

    return sorted(files, key=lambda item: (getattr(item, "path", "") or "").lower())


def _build_zip_bytes(w, base_dir: str, file_entries):
    zipped = io.BytesIO()
    failures = []

    with zipfile.ZipFile(zipped, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in file_entries:
            remote_path = (getattr(f, "path", "") or "").rstrip("/")
            if not remote_path:
                continue
            rel_name = posixpath.relpath(remote_path, start=base_dir)
            if rel_name in (".", ""):
                rel_name = _entry_name(f)
            try:
                zf.writestr(rel_name, _download_file_bytes(w, remote_path))
            except Exception as err:
                failures.append((remote_path, str(err)))

    zipped.seek(0)
    return zipped.getvalue(), failures, len(file_entries)


# Subida de archivos a Volumen Databricks
def upload_to_databricks_volume(local_file, dbx_path):
    w = _workspace_client()
    w.files.create_directory(posixpath.dirname(dbx_path))
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

def _enum_value(value):
    return value.value if hasattr(value, "value") else str(value) if value is not None else None


def _try_parse_json(text):
    if text is None:
        return None
    if isinstance(text, (dict, list)):
        return text
    if not isinstance(text, str):
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def run_job_and_wait(month_label: str):
    if JOB_ID_INT is None:
        raise RuntimeError("JOB_ID no configurado o inválido (.env).")

    run_id = None
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

        run_id = run.run_id
        while True:
            run_state = w.jobs.get_run(run_id)
            life_cycle = _enum_value(getattr(run_state.state, "life_cycle_state", None))
            if life_cycle in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
                break
            time.sleep(5)

        result_state = _enum_value(getattr(run_state.state, "result_state", None))
        state_message = getattr(run_state.state, "state_message", None)

        if result_state == "SUCCESS":
            metrics = None
            try:
                output = w.jobs.get_run_output(run_id)
                notebook_output = getattr(output, "notebook_output", None)
                raw_result = getattr(notebook_output, "result", None) if notebook_output else None
                parsed = _try_parse_json(raw_result)
                if isinstance(parsed, dict):
                    metrics = parsed.get("metrics", parsed)
                elif parsed is not None:
                    metrics = parsed
                else:
                    metrics = raw_result
            except Exception:
                metrics = None

            return run_id, metrics

        parsed_error = _try_parse_json(state_message)
        if parsed_error is not None:
            details = json.dumps(parsed_error, ensure_ascii=False)
        else:
            details = state_message or "Sin detalles"

        raise RuntimeError(
            f"Job falló. run_id={run_id} | life_cycle={life_cycle} | result_state={result_state} | detalle={details}"
        )
    except DatabricksError as dbx_err:
        raise RuntimeError(f"Error específico de Databricks (run_id={run_id}): {dbx_err}")
    except Exception as e:
        raise RuntimeError(f"Error al autenticar o ejecutar el Job (run_id={run_id}): {e}")

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
                    dbx_dest = posixpath.join(in_path, f.name)
                    upload_to_databricks_volume(tmp.name, dbx_dest)
                    subidos += 1

            with st.status("Ejecutando notebook en Databricks…", expanded=True) as status:
                st.write(f" Input: `{in_path}`")
                st.write(f" Subidos {subidos} archivo(s) a Databricks Volume.")
                st.write(" Lanzando Job…")
                start = time.time()
                run_id, metrics = run_job_and_wait(m_label)
                elapsed = time.time() - start
                st.write(f" Job terminado. run_id = `{run_id}` en {elapsed:,.1f}s.")
                if metrics is not None:
                    if isinstance(metrics, (dict, list)):
                        st.write(" Métricas / salida del notebook:")
                        st.json(metrics)
                    else:
                        st.write(f" Salida notebook: {metrics}")
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

    # Estado de navegación y cache de descargas
    if "current_path" not in st.session_state:
        st.session_state["current_path"] = None
    if "download_cache_path" not in st.session_state:
        st.session_state["download_cache_path"] = None
    if "download_cache_items" not in st.session_state:
        st.session_state["download_cache_items"] = []
    if "download_cache_failures" not in st.session_state:
        st.session_state["download_cache_failures"] = []
    if "zip_cache_path" not in st.session_state:
        st.session_state["zip_cache_path"] = None
    if "zip_cache_data" not in st.session_state:
        st.session_state["zip_cache_data"] = None
    if "zip_cache_failures" not in st.session_state:
        st.session_state["zip_cache_failures"] = []
    if "zip_cache_total_files" not in st.session_state:
        st.session_state["zip_cache_total_files"] = 0

    # Si el usuario cambia mes/año, limpiar navegación y cache
    if "last_out_path" not in st.session_state or st.session_state["last_out_path"] != out_path2:
        st.session_state["current_path"] = None
        st.session_state["last_out_path"] = out_path2
        st.session_state["download_cache_path"] = None
        st.session_state["download_cache_items"] = []
        st.session_state["download_cache_failures"] = []
        st.session_state["zip_cache_path"] = None
        st.session_state["zip_cache_data"] = None
        st.session_state["zip_cache_failures"] = []
        st.session_state["zip_cache_total_files"] = 0

    st.markdown("**Navega y descarga archivos de salida:**")
    buscar = st.button("Buscar", use_container_width=True)
    w = _workspace_client()

    # Solo mostrar resultados tras buscar
    if buscar or st.session_state["current_path"]:
        # Inicializar navegación si es la primera vez
        if st.session_state["current_path"] is None:
            st.session_state["current_path"] = out_path2

        # Botón para resetear navegación
        if st.button("Ir a carpeta raíz", use_container_width=True, key="reset_nav"):
            st.session_state["current_path"] = out_path2
            st.session_state["download_cache_path"] = None
            st.session_state["download_cache_items"] = []
            st.session_state["download_cache_failures"] = []
            st.session_state["zip_cache_path"] = None
            st.session_state["zip_cache_data"] = None
            st.session_state["zip_cache_failures"] = []
            st.session_state["zip_cache_total_files"] = 0

        current_path = st.session_state["current_path"]

        # Si cambió ruta, invalidar cache para evitar duplicados o datos viejos
        if st.session_state["download_cache_path"] != current_path:
            st.session_state["download_cache_items"] = []
            st.session_state["download_cache_failures"] = []
        if st.session_state["zip_cache_path"] != current_path:
            st.session_state["zip_cache_data"] = None
            st.session_state["zip_cache_failures"] = []
            st.session_state["zip_cache_total_files"] = 0

        try:
            entries = _dedupe_entries(list(w.files.list_directory_contents(current_path)))
            dirs = sorted([e for e in entries if _entry_is_dir(e)], key=lambda entry: _entry_name(entry).lower())
            all_files = sorted([e for e in entries if not _entry_is_dir(e)], key=lambda entry: _entry_name(entry).lower())
            files, latest_date = _filter_latest_visible_entries(all_files)

            st.caption(f"Ruta actual: {current_path}")
            st.caption(f"Carpetas: {len(dirs)} | Documentos visibles: {len(files)}")
            if latest_date is not None:
                st.caption(
                    "Fecha más reciente detectada: "
                    f"{latest_date[0]:04d}_{latest_date[1]:02d}_{latest_date[2]:02d}"
                )

            if current_path != out_path2:
                parent_path = posixpath.dirname(current_path.rstrip("/"))
                if parent_path.startswith(out_path2):
                    if st.button("Subir un nivel", use_container_width=True, key="up_nav"):
                        st.session_state["current_path"] = parent_path
                        st.rerun()

            if not dirs and not all_files:
                st.warning("No hay archivos ni carpetas en esta ruta.")
            else:
                # Mostrar carpetas
                for d in dirs:
                    dir_name = _entry_name(d)
                    if st.button(f" {dir_name}", key=f"dir_{d.path}", use_container_width=True):
                        st.session_state["current_path"] = d.path
                        st.rerun()

                if not files and all_files:
                    st.info("Se ocultaron archivos TXT; se muestran documentos de la fecha más reciente.")

                # Lista de archivos visibles en esta carpeta (rápido, sin descargar todavía)
                for f in files:
                    st.caption(f"- {_entry_name(f)}")

                load_all_now = False
                build_zip_now = False
                if files:
                    col_load, col_zip = st.columns(2)
                    load_all_now = col_load.button("Cargar todos (misma carpeta)", use_container_width=True, key="load_all_now")
                    build_zip_now = col_zip.button("Preparar ZIP de documentos", use_container_width=True, key="build_zip_now")

                if load_all_now:
                    loaded = []
                    failures = []
                    progress = st.progress(0)
                    status = st.empty()
                    total = len(files)
                    for idx, f in enumerate(files, start=1):
                        fpath = (getattr(f, "path", "") or "").rstrip("/")
                        fname = _entry_name(f)
                        status.caption(f"Cargando {idx}/{total}: {fname}")
                        try:
                            data = _download_file_bytes(w, fpath)
                            mime, _ = mimetypes.guess_type(fname)
                            loaded.append(
                                {
                                    "path": fpath,
                                    "name": fname,
                                    "bytes": data,
                                    "mime": mime or "application/octet-stream",
                                }
                            )
                        except Exception as err:
                            failures.append((fname, str(err)))
                        progress.progress(int((idx / total) * 100))

                    st.session_state["download_cache_path"] = current_path
                    st.session_state["download_cache_items"] = loaded
                    st.session_state["download_cache_failures"] = failures

                if build_zip_now:
                    with st.spinner("Preparando ZIP con los documentos visibles..."):
                        zip_bytes, zip_failures, zip_total_files = _build_zip_bytes(w, current_path, files)
                    st.session_state["zip_cache_path"] = current_path
                    st.session_state["zip_cache_data"] = zip_bytes
                    st.session_state["zip_cache_failures"] = zip_failures
                    st.session_state["zip_cache_total_files"] = zip_total_files

                # Mostrar descargas individuales solo cuando ya estén todas cargadas
                if st.session_state["download_cache_path"] == current_path and st.session_state["download_cache_items"]:
                    st.success(f"Archivos cargados para descarga: {len(st.session_state['download_cache_items'])}")
                    for item in st.session_state["download_cache_items"]:
                        st.download_button(
                            label=f"⬇Descargar {item['name']}",
                            data=item["bytes"],
                            file_name=item["name"],
                            mime=item["mime"],
                            key=f"dl_{item['path']}",
                            use_container_width=True,
                        )

                if st.session_state["download_cache_path"] == current_path and st.session_state["download_cache_failures"]:
                    st.warning(f"No se pudieron cargar {len(st.session_state['download_cache_failures'])} archivo(s).")
                    for fname, err in st.session_state["download_cache_failures"]:
                        st.caption(f"- {fname}: {err}")

                if st.session_state["zip_cache_path"] == current_path and st.session_state["zip_cache_data"]:
                    zip_filename = f"outputs_{mes2}_{year2}_{posixpath.basename(current_path.rstrip('/')) or 'carpeta'}.zip"
                    st.success(f"ZIP listo con {st.session_state['zip_cache_total_files']} documento(s).")
                    st.download_button(
                        label="⬇Descargar documentos visibles en ZIP",
                        data=st.session_state["zip_cache_data"],
                        file_name=zip_filename,
                        mime="application/zip",
                        key="dl_zip_all",
                        use_container_width=True,
                    )

                if st.session_state["zip_cache_path"] == current_path and st.session_state["zip_cache_failures"]:
                    st.warning(f"No se pudieron incluir {len(st.session_state['zip_cache_failures'])} archivo(s) en el ZIP.")
                    for fpath, err in st.session_state["zip_cache_failures"]:
                        st.caption(f"- {fpath}: {err}")
        except Exception as e:
            st.error(f"No se pudieron listar/descargar outputs: {e}")