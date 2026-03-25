# VAT Refund – Lector/Entrenador

## Guía Rápida de Inicio

### 1. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 2. Configurar variables de entorno
```bash
# En Windows:
copy example.env .env

# En Linux/Mac:
cp example.env .env
```
Edita el archivo `.env` con tus credenciales de Databricks.

### 3. Ejecutar la aplicación
```bash
py -m streamlit run app.py
```

---

## Interfaz de Usuario

La aplicación tiene **3 pestañas principales**:

### 🔹 Pestaña "Entradas"
- **Función**: Navegar y descargar archivos de entrada
- Selecciona el **Mes** y **Año**
- **Botones de navegación**:
  - `Subir un nivel` - Sube a la carpeta padre
  - Botones individuales - Abre carpetas o descarga archivos
  - `Cargar todos ahora` - Descarga todos los archivos
  - `Construir ZIP ahora` - Comprime todos en un ZIP

### 🔹 Pestaña "Procesar"
- **Función**: Sube archivos y ejecuta el proceso principal
- **Pasos**:
  1. Selecciona el **Mes** (Enero-Diciembre)
  2. Selecciona el **Año** (2024-2032)
  3. Carga los **archivos de entrada** (puedes arrastrar múltiples)
  4. Presiona el botón **"Subir y ejecutar"** (azul/principal)
- Los archivos deben cumplir con los 8 patrones de nombres requeridos

### 🔹 Pestaña "Resultados"
- **Función**: Descargar archivos de salida del proceso
- Selecciona el mismo **Mes** y **Año** del proceso ejecutado
- **Botones de navegación**:
  - `Subir un nivel` - Sube a la carpeta padre
  - Botones individuales - Descarga archivos
  - `Cargar todos ahora` - Descarga todos los resultados
  - `Construir ZIP ahora` - Comprime todos en un ZIP

---

## Patrones de Nombres de Archivo

Los archivos de entrada deben seguir uno de estos 8 patrones:

| Patrón | Ejemplo | Extensión |
|--------|---------|-----------|
| 1) | `1) Reporte QRY343 1225.xlsx` | .xlsx |
| 2) | `2) Reporte QRY100 1225 MN.xlsx` | .xlsx |
| 3) | `3) Base de datos 1225 (1).xlsx` | .xlsx |
| 4) | `4) Base de datos 1225.xlsx` | .xlsx |
| 5) | `5) Estado de cuenta MN 8762 1225.xlsx` | .xlsx |
| 6) | `6) Registros auxiliares 1225.xlsx` | .xlsx |
| 7) | `7) Estado de cuenta MN 8762 1225 (1).pdf` | .pdf |
| 8) | `8) Tipos de Cambio 1225 (1).xlsx` | .xlsx |

**Nota**: El formato MMYY es Mes (01-12) + Año (ultimos 2 dígitos)

---

## Troubleshooting

- **Error de conexión Databricks**: Verifica las credenciales en `.env`
- **Archivo rechazado**: Asegúrate de que el nombre cumple con uno de los 8 patrones
- **Proceso lento**: Los archivos grandes pueden tardar, revisa el estado en la barra de progreso
