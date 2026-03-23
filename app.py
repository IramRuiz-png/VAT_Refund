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


# ---------- Validación de archivos según 8 patrones estrictos ----------

MMYY_PATTERN = r"(?:0[1-9]|1[0-2])\d{2}"

FILENAME_RULES = {
    1: {
        "regex": rf"^1\)\s.*?\bQRY\d{{3}}\b\s+{MMYY_PATTERN}\.xlsx$",
        "example": "1) Reporte QRY343 1225.xlsx",
        "extension": ".xlsx",
    },
    2: {
        "regex": rf"^2\)\s.*?\bQRY\d{{3}}\b\s+{MMYY_PATTERN}\s+(?:MN|USD)\.xlsx$",
        "example": "2) Reporte QRY100 1225 MN.xlsx",
        "extension": ".xlsx",
    },
    3: {
        "regex": rf"^3\)\s.*?\b{MMYY_PATTERN}(?:\s\(1\))?\.xlsx$",
        "example": "3) Base de datos 1225 (1).xlsx",
        "extension": ".xlsx",
    },
    4: {
        "regex": rf"^4\)\s.*?\b{MMYY_PATTERN}\.xlsx$",
        "example": "4) Base de datos 1225.xlsx",
        "extension": ".xlsx",
    },
    5: {
        "regex": rf"^5\)\s.*?\b(?:MN|USD)\s+\d{{4}}\s+{MMYY_PATTERN}\.xlsx$",
        "example": "5) Estado de cuenta MN 8762 1225.xlsx",
        "extension": ".xlsx",
    },
    6: {
        "regex": rf"^6\)\s.*?\b{MMYY_PATTERN}\.xlsx$",
        "example": "6) Registros auxiliares 1225.xlsx",
        "extension": ".xlsx",
    },
    7: {
        "regex": rf"^7\)\s.*?\b(?:MN|USD)\s+\d{{4}}\s+{MMYY_PATTERN}(?:\s\(1\))?\.pdf$",
        "example": "7) Estado de cuenta MN 8762 1225 (1).pdf",
        "extension": ".pdf",
    },
    8: {
        "regex": rf"^8\)\s.*?\b{MMYY_PATTERN}(?:\s\(1\))?\.xlsx$",
        "example": "8) Tipos de Cambio 1225 (1).xlsx",
        "extension": ".xlsx",
    },
}


def _is_valid_mmyy(value: str) -> bool:
    return bool(re.fullmatch(MMYY_PATTERN, value or ""))


def _explain_pattern_mismatch(filename: str, pattern_number: int) -> str:
    stem, ext = os.path.splitext(filename)
    rule = FILENAME_RULES[pattern_number]

    if ext.lower() != rule["extension"]:
        return f"Extensión inválida: se esperaba {rule['extension']}"

    if pattern_number in (1, 2):
        if not re.search(r"\bQRY\d{3}\b", filename, re.IGNORECASE):
            return "Falta QRY con 3 dígitos (ejemplo: QRY343)"

    if pattern_number in (2, 5, 7):
        if not re.search(r"\b(?:MN|USD)\b", filename, re.IGNORECASE):
            return "Falta moneda válida (solo MN o USD)"

    if pattern_number in (5, 7):
        if not re.search(r"\b(?:MN|USD)\s+\d{4}\b", filename, re.IGNORECASE):
            return "Después de la moneda deben venir exactamente 4 dígitos"

    if pattern_number == 4 and re.search(r"\(1\)", filename):
        return "Este patrón no permite '(1)'"

    if pattern_number in (3, 8):
        if re.search(r"\(\d+\)", filename) and not re.search(r"\(1\)", filename):
            return "Solo se permite '(1)' como sufijo opcional"

    # Validación de fecha MMYY (mes 01-12)
    date_candidate = None
    if pattern_number in (2, 5, 7):
        m = re.search(r"\b(\d{4})\s+(?:MN|USD)(?:\s*\(1\))?$", stem, re.IGNORECASE)
        if m:
            date_candidate = m.group(1)
        else:
            m = re.search(r"\b(\d{4})(?:\s*\(1\))?$", stem)
            if m:
                date_candidate = m.group(1)
    else:
        m = re.search(r"\b(\d{4})(?:\s*\(1\))?$", stem)
        if m:
            date_candidate = m.group(1)

    if not date_candidate:
        return "Falta fecha MMYY al final (mes válido 01-12)"

    if not _is_valid_mmyy(date_candidate):
        return f"Fecha inválida '{date_candidate}': debe ser MMYY con mes entre 01 y 12"

    return "El orden de elementos no coincide con el patrón requerido"


def validate_filename(filename: str) -> tuple[bool, int | None, str]:
    if not filename or not isinstance(filename, str):
        return False, None, "Nombre de archivo vacío o inválido"

    prefix = re.match(r"^\s*(\d+)\)\s", filename)
    if not prefix:
        return (
            False,
            None,
            "Debe iniciar con N) (1..8). Ejemplo: 1) Reporte QRY343 1225.xlsx",
        )

    pattern_number = int(prefix.group(1))
    if pattern_number not in FILENAME_RULES:
        return (
            False,
            None,
            "Número de patrón inválido: solo se permiten 1) a 8)",
        )

    rule = FILENAME_RULES[pattern_number]
    if re.match(rule["regex"], filename, re.IGNORECASE):
        return True, pattern_number, f"Cumple patrón {pattern_number}"

    reason = _explain_pattern_mismatch(filename, pattern_number)
    return (
        False,
        pattern_number,
        f"Patrón {pattern_number}: {reason}. Ejemplo correcto: {rule['example']}",
    )


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


def render_volume_browser(section_key: str, root_path: str, mes: str, year: int, zip_prefix: str, not_found_label: str):
    current_key = f"{section_key}_current_path"
    last_root_key = f"{section_key}_last_root"
    download_cache_path_key = f"{section_key}_download_cache_path"
    download_cache_items_key = f"{section_key}_download_cache_items"
    download_cache_failures_key = f"{section_key}_download_cache_failures"
    zip_cache_path_key = f"{section_key}_zip_cache_path"
    zip_cache_data_key = f"{section_key}_zip_cache_data"
    zip_cache_failures_key = f"{section_key}_zip_cache_failures"
    zip_cache_total_key = f"{section_key}_zip_cache_total_files"

    if current_key not in st.session_state:
        st.session_state[current_key] = None
    if download_cache_path_key not in st.session_state:
        st.session_state[download_cache_path_key] = None
    if download_cache_items_key not in st.session_state:
        st.session_state[download_cache_items_key] = []
    if download_cache_failures_key not in st.session_state:
        st.session_state[download_cache_failures_key] = []
    if zip_cache_path_key not in st.session_state:
        st.session_state[zip_cache_path_key] = None
    if zip_cache_data_key not in st.session_state:
        st.session_state[zip_cache_data_key] = None
    if zip_cache_failures_key not in st.session_state:
        st.session_state[zip_cache_failures_key] = []
    if zip_cache_total_key not in st.session_state:
        st.session_state[zip_cache_total_key] = 0

    if last_root_key not in st.session_state or st.session_state[last_root_key] != root_path:
        st.session_state[current_key] = None
        st.session_state[last_root_key] = root_path
        st.session_state[download_cache_path_key] = None
        st.session_state[download_cache_items_key] = []
        st.session_state[download_cache_failures_key] = []
        st.session_state[zip_cache_path_key] = None
        st.session_state[zip_cache_data_key] = None
        st.session_state[zip_cache_failures_key] = []
        st.session_state[zip_cache_total_key] = 0

    st.markdown("**Navega y descarga archivos:**")
    buscar = st.button("Buscar", use_container_width=True, key=f"{section_key}_buscar")
    w = _workspace_client()

    if buscar or st.session_state[current_key]:
        if st.session_state[current_key] is None:
            st.session_state[current_key] = root_path

        if st.button("Ir a carpeta raíz", use_container_width=True, key=f"{section_key}_reset_nav"):
            st.session_state[current_key] = root_path
            st.session_state[download_cache_path_key] = None
            st.session_state[download_cache_items_key] = []
            st.session_state[download_cache_failures_key] = []
            st.session_state[zip_cache_path_key] = None
            st.session_state[zip_cache_data_key] = None
            st.session_state[zip_cache_failures_key] = []
            st.session_state[zip_cache_total_key] = 0

        current_path = st.session_state[current_key]

        if st.session_state[download_cache_path_key] != current_path:
            st.session_state[download_cache_items_key] = []
            st.session_state[download_cache_failures_key] = []
        if st.session_state[zip_cache_path_key] != current_path:
            st.session_state[zip_cache_data_key] = None
            st.session_state[zip_cache_failures_key] = []
            st.session_state[zip_cache_total_key] = 0

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

            if current_path != root_path:
                parent_path = posixpath.dirname(current_path.rstrip("/"))
                if parent_path.startswith(root_path):
                    if st.button("Subir un nivel", use_container_width=True, key=f"{section_key}_up_nav"):
                        st.session_state[current_key] = parent_path
                        st.rerun()

            if not dirs and not all_files:
                st.warning("No hay archivos ni carpetas en esta ruta.")
            else:
                for d in dirs:
                    dir_name = _entry_name(d)
                    if st.button(f" {dir_name}", key=f"{section_key}_dir_{d.path}", use_container_width=True):
                        st.session_state[current_key] = d.path
                        st.rerun()

                if not files and all_files:
                    st.info("Se ocultaron archivos TXT; se muestran documentos de la fecha más reciente.")

                for f in files:
                    st.caption(f"- {_entry_name(f)}")

                load_all_now = False
                build_zip_now = False
                if files:
                    col_load, col_zip = st.columns(2)
                    load_all_now = col_load.button(
                        "Cargar todos (misma carpeta)",
                        use_container_width=True,
                        key=f"{section_key}_load_all_now",
                    )
                    build_zip_now = col_zip.button(
                        "Preparar ZIP de documentos",
                        use_container_width=True,
                        key=f"{section_key}_build_zip_now",
                    )

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

                    st.session_state[download_cache_path_key] = current_path
                    st.session_state[download_cache_items_key] = loaded
                    st.session_state[download_cache_failures_key] = failures

                if build_zip_now:
                    with st.spinner("Preparando ZIP con los documentos visibles..."):
                        zip_bytes, zip_failures, zip_total_files = _build_zip_bytes(w, current_path, files)
                    st.session_state[zip_cache_path_key] = current_path
                    st.session_state[zip_cache_data_key] = zip_bytes
                    st.session_state[zip_cache_failures_key] = zip_failures
                    st.session_state[zip_cache_total_key] = zip_total_files

                if st.session_state[download_cache_path_key] == current_path and st.session_state[download_cache_items_key]:
                    st.success(f"Archivos cargados para descarga: {len(st.session_state[download_cache_items_key])}")
                    for item in st.session_state[download_cache_items_key]:
                        st.download_button(
                            label=f"Descargar {item['name']}",
                            data=item["bytes"],
                            file_name=item["name"],
                            mime=item["mime"],
                            key=f"{section_key}_dl_{item['path']}",
                            use_container_width=True,
                        )

                if st.session_state[download_cache_path_key] == current_path and st.session_state[download_cache_failures_key]:
                    st.warning(f"No se pudieron cargar {len(st.session_state[download_cache_failures_key])} archivo(s).")
                    for fname, err in st.session_state[download_cache_failures_key]:
                        st.caption(f"- {fname}: {err}")

                if st.session_state[zip_cache_path_key] == current_path and st.session_state[zip_cache_data_key]:
                    zip_filename = f"{zip_prefix}_{mes}_{year}_{posixpath.basename(current_path.rstrip('/')) or 'carpeta'}.zip"
                    st.success(f"ZIP listo con {st.session_state[zip_cache_total_key]} documento(s).")
                    st.download_button(
                        label="Descargar documentos visibles en ZIP",
                        data=st.session_state[zip_cache_data_key],
                        file_name=zip_filename,
                        mime="application/zip",
                        key=f"{section_key}_dl_zip_all",
                        use_container_width=True,
                    )

                if st.session_state[zip_cache_path_key] == current_path and st.session_state[zip_cache_failures_key]:
                    st.warning(f"No se pudieron incluir {len(st.session_state[zip_cache_failures_key])} archivo(s) en el ZIP.")
                    for fpath, err in st.session_state[zip_cache_failures_key]:
                        st.caption(f"- {fpath}: {err}")
        except Exception as e:
            st.error(f"No se pudieron listar/descargar {not_found_label}: {e}")


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

# ---------- Manual de usuario (popover) ----------

with st.popover(" Manual de uso"):
    st.markdown("""
##  Manual de uso
*Lee esto antes de empezar — está escrito para que cualquier persona pueda usar la app sin perderse.*

---

###  ¿Para qué sirve esta app?

Con esta app puedes hacer **tres cosas**:

1. **Subir archivos de entrada** a Databricks para procesarlos.
2. **Procesar esos archivos** lanzando el job automático.
3. **Ver y descargar** tanto los archivos que subiste (**Entradas**) como los resultados del proceso (**Resultados**).

>  Los archivos deben tener un **nombre con formato especial**. Si el nombre no cumple, el archivo **no se sube**.

---

###  Nombres de archivo válidos

El nombre debe empezar con un número del **1 al 8** seguido de `)` y un espacio.  
La **fecha** va en formato MMYY: 2 dígitos de mes (01–12) + 2 dígitos de año. Ej: `1225` = diciembre 2025.

| Patrón | Extensión | Ejemplo correcto |
|--------|-----------|-----------------|
| `1)` | .xlsx | `1) Reporte QRY343 1225.xlsx` |
| `2)` | .xlsx | `2) Reporte QRY100 1225 MN.xlsx` |
| `3)` | .xlsx | `3) Base de datos 1225 (1).xlsx` |
| `4)` | .xlsx | `4) Base de datos 1225.xlsx` |
| `5)` | .xlsx | `5) Estado de cuenta MN 8762 1225.xlsx` |
| `6)` | .xlsx | `6) Registros auxiliares 1225.xlsx` |
| `7)` | .pdf  | `7) Estado de cuenta MN 8762 1225 (1).pdf` |
| `8)` | .xlsx | `8) Tipos de Cambio 1225 (1).xlsx` |

- Monedas válidas: solo **MN** o **USD**.
- El `(1)` al final es **opcional** en patrones 3, 7 y 8. El patrón 4 **no lo permite**.
- Los nombres no distinguen mayúsculas de minúsculas.

---

###  Pestaña: Entradas

Aquí ves y descargas los **archivos que ya fueron subidos** para un mes/año.

1. Elige **Mes** y **Año**.
2. Clic en **Buscar**.
3. Navega las carpetas haciendo clic en ellas.  
   - **"Subir un nivel"** → regresa a la carpeta anterior.  
   - **"Ir a carpeta raíz"** → regresa al inicio.
4. Una vez dentro de la carpeta con los archivos:  
   - **"Cargar todos"** → carga los archivos para descarga uno a uno.  
   - **"Preparar ZIP"** → empaqueta todo en un solo .zip descargable.
5. Haz clic en el botón de descarga que aparece.

---

###  Pestaña: Procesar

Aquí subes archivos nuevos y lanzas el proceso.

1. Elige **Mes** y **Año** al que pertenecen los archivos.
2. Arrastra o selecciona los archivos. Puedes subir **varios a la vez**.  
   - Si algún nombre es incorrecto, verás el error exacto y un ejemplo de cómo debería llamarse.  
   - Los archivos inválidos **no se suben**; los válidos sí.
3. Clic en **"Subir y ejecutar"**.
4. Aparece un panel de estado con el avance; espera a que diga **"Proceso finalizado"**.
5. Ve a la pestaña **Resultados** para descargar los archivos de salida.

>  No cierres ni recargues la página mientras el proceso corre.

---

###  Pestaña: Resultados

Aquí descargas los **archivos generados por el proceso**. Funciona igual que Entradas:

1. Elige **Mes** y **Año** → clic en **Buscar**.
2. Navega las carpetas y usa **"Cargar todos"** o **"Preparar ZIP"**.
3. Descarga con el botón que aparece.

>  Si acabas de procesar, espera unos segundos antes de buscar.

---

###  Errores frecuentes

**"No cumple con ninguno de los 8 patrones"**  
→ Revisa que el nombre empiece con `N)` (del 1 al 8), tenga fecha MMYY con mes 01–12 y la extensión correcta.

**"Extensión inválida"**  
→ Cada patrón pide una extensión específica. El patrón 7 solo acepta `.pdf`; los demás `.xlsx`.

**"Falta QRY con 3 dígitos"**  
→ Incluye algo como `QRY123` en el nombre (QRY + exactamente 3 números).

**"Falta moneda válida"**  
→ Solo se aceptan `MN` o `USD`.

**"Fecha inválida"**  
→ Los primeros 2 dígitos son el mes. `1325` no es válido porque no existe el mes 13.

**"Job falló"**  
→ Error en Databricks. Anota el `run_id` y repórtalo al equipo técnico.

**No aparecen archivos**  
→ Verifica que el mes y año sean correctos y que ya se hayan subido/procesado archivos para ese periodo.
""")

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

tab3, tab1, tab2 = st.tabs([" Entradas", " Procesar", " Resultados"])

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

        # Validar archivos antes de subirlos
        archivos_válidos = []
        archivos_inválidos = []
        
        for f in files:
            es_válido, patron_num, mensaje = validate_filename(f.name)
            if es_válido:
                archivos_válidos.append(f)
            else:
                archivos_inválidos.append((f.name, mensaje))
        
        if archivos_inválidos:
            st.warning("⚠️ Los siguientes archivos NO cumplen con los patrones requeridos y NO serán subidos:")
            for nombre, msg in archivos_inválidos:
                st.code(f"{nombre}\n→ {msg}", language=None)
        
        if not archivos_válidos:
            st.error("❌ Ninguno de los archivos subidos cumple con los patrones requeridos.")
            st.stop()
        
        if archivos_inválidos:
            st.info(f"✓ Se procederá a subir solo {len(archivos_válidos)} archivo(s) válido(s)")

        try:
            in_path, out_path = io_paths(m_label)
            # Guardar archivos temporales y subirlos a Databricks Volume
            import tempfile
            subidos = 0
            for f in archivos_válidos:
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
    render_volume_browser(
        section_key="outputs",
        root_path=out_path2,
        mes=mes2,
        year=year2,
        zip_prefix="outputs",
        not_found_label="outputs",
    )


# ---------- Tab Entradas ----------

with tab3:
    st.subheader("Descargar entradas")
    colM3, colY3 = st.columns([2,1])
    with colM3:
        mes3 = st.selectbox("Mes", MESES, index=11, key="m3")
    with colY3:
        year3 = st.number_input("Año", min_value=YEAR_MIN, max_value=YEAR_MAX, value=2025, step=1, key="y3")
    m_label3 = month_label(mes3, year3)
    in_path3, _ = io_paths(m_label3)
    render_volume_browser(
        section_key="inputs",
        root_path=in_path3,
        mes=mes3,
        year=year3,
        zip_prefix="entradas",
        not_found_label="entradas",
    )