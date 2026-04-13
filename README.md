# ScoutingAgent

ScoutingAgent es una plataforma de análisis de datos futbolísticos asistida por IA que combina:

- ingesta y transformación de datos de eventos (Wyscout -> BigQuery),
- análisis de posesiones y métricas de jugador,
- generación automática de reportes (LangGraph + Vertex AI),
- interfaz interactiva en Streamlit (chatbot, scouting individual, comparación y RAG).

El objetivo es acelerar el trabajo de analistas y *scouts* con un flujo end-to-end reproducible: desde datos crudos hasta insights accionables.

## Características principales

- **Pipeline de datos por capas** (`bronze/silver/gold`) con Bruin.
- **Análisis táctico y estadístico** de posesiones y eventos.
- **Generación de reportes PDF** con secciones narrativas y visualizaciones.
- **RAG sobre reportes de jugadores** para consultas comparativas.
- **Chatbot de análisis** conectado a servicios de datos.

## Arquitectura (alto nivel)

1. **Ingesta**: extracción de datos de eventos y dimensiones.
2. **Transformación**: normalización y enriquecimiento en BigQuery.
3. **Lógica de scouting**: cálculo de métricas, clasificación y descripción.
4. **Orquestación IA**: flujos LangGraph para producir salidas estructuradas.
5. **Presentación**: app Streamlit con páginas especializadas.

## Estructura del repositorio

- `app.py`: entrada principal de la app Streamlit.
- `pages/`: páginas funcionales (chatbot, scouting, comparación, RAG).
- `services/`: acceso a datos, LLM, secretos runtime y lógica de negocio.
- `langgraph_flow/`: grafo y nodos para generación de reportes.
- `bruin-pipeline/`: assets de pipeline y SQL para capas de datos.
- `prompts/`: plantillas/prompts versionados para generación textual.
- `visualization/`: utilidades de visualización.
- `thesis/`: documentación académica y material LaTeX del proyecto.

## Requisitos

- Python `>=3.11`
- Entorno con acceso a servicios GCP/Vertex/BigQuery (según uso)
- Credenciales y variables de entorno configuradas localmente

## Instalación rápida

```bash
# desde la raíz del proyecto
uv sync
```

Alternativa con `pip`:

```bash
pip install -e .
```

## Ejecución local

```bash
streamlit run app.py
```

La app abre un menú con:

- `Chatbot`
- `Player scouting`
- `Report RAG`
- `Player comparison`

## Configuración de entorno

1. Crear archivo local `.env` (no versionado).
2. Partir de `.env.example` como plantilla.
3. Configurar claves/IDs de proyecto y credenciales de servicios.

El proyecto está preparado para leer secretos desde variables de entorno y, en cloud, desde `st.secrets`.

## Pipeline de datos (Bruin)

Dentro de `bruin-pipeline/` se definen los assets para cargar y transformar datos en distintas capas.

Flujo típico:

```bash
cd bruin-pipeline
bruin validate .
bruin run .
```

## Seguridad y buenas prácticas

- No subas `.env`, `.secrets/` ni credenciales.
- Evita versionar artefactos de compilación (`.aux`, logs, caches).
- Revisa siempre `git status` antes de hacer commit/push.

## Estado del proyecto

ScoutingAgent está en evolución activa. Algunas partes del sistema están optimizadas para el caso de uso de tesis/prototipo avanzado y pueden requerir ajustes para producción estricta (monitorización, hardening y CI/CD completo).
