demanda.py
```
import logging
from typing import TypedDict, Optional
from datetime import datetime
import requests
import json
from dotenv import load_dotenv
import os

# --- Logging Setup ---
logging.basicConfig(
    filename="logs/demanda.log",  # Log file name
    level=logging.INFO,  # Set the minimum level to log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",  # Set date format
)
logger = logging.getLogger(__name__)  # Get a logger instance for this module
# --- End Logging Setup ---


SIN = [
    "Central",
    "Noreste",
    "Noroeste",
    "Norte",
    "Occidental",
    "Oriental",
    "Peninsular",
]
BCS = ["Baja California Sur"]
BCA = ["Baja California"]

# Diccionario de números de gerencia
MAPEO_GERENCIAS = {
    1: "Baja California",
    2: "Baja California Sur",
    3: "Central",
    4: "Noreste",
    5: "Noroeste",
    6: "Norte",
    7: "Occidental",
    8: "Oriental",
    9: "Peninsular",
}

URL = "https://www.cenace.gob.mx/GraficaDemanda.aspx/obtieneValoresTotal"

API_URL = "http://127.0.0.1:5001"  # Make sure this is accessible

CODE_STATUS = [200, 201]


# Establece el tipo de datos para la gerencia
class Gerencia(TypedDict):
    hora: str
    valorDemanda: str
    valorGeneracion: str
    valorEnlace: Optional[None]
    valorPronostico: str


# Establece el tipo de datos para la respuesta de la API
class DataSchema(TypedDict):
    FechaOperacion: str
    HoraOperacion: int
    Demanda: int
    Generacion: int
    Enlace: Optional[None]
    Pronostico: int
    Gerencia: str
    Sistema: str
    FechaCreacion: str
    FechaModificacion: str

    # Pronostico = a, Demanda = b


def send_telegram_message(bot_token, chat_id, message):
    # Send a message to a Telegram chat.
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        logger.info("Telegram message sent successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Faild to send Telegram message: {e}")


def diferencia_en_porcentaje(pronostico: int, demanda: int) -> tuple[float, str]:
    # Added check for pronostico == 0 to avoid ZeroDivisionError
    if pronostico == 0:
        logger.warning("Pronostico is zero, cannot calculate percentage difference.")
        return (0.0, "división por cero")
    diferencia = ((demanda - pronostico) / pronostico) * 100
    direccion = (
        "más alta" if diferencia > 0 else "más baja" if diferencia < 0 else "igual"
    )
    return (abs(round(diferencia, 2)), direccion)


def obtener_sistema(gerencia_name: str) -> str:
    if gerencia_name in BCA:
        return "BCA"
    elif gerencia_name in BCS:
        return "BCS"
    elif gerencia_name in SIN:
        return "SIN"
    logger.warning("Could not determine system for gerencia: %s", gerencia_name)
    return ""


def enviar_peticion(
    gerencia: Gerencia, hora: int, gerencia_name: str
) -> Optional[requests.Response]:
    fecha_operacion = datetime.today().strftime("%Y-%m-%d")

    data: DataSchema = {
        "FechaOperacion": fecha_operacion,
        "HoraOperacion": hora,
        "Demanda": int(gerencia["valorDemanda"]),
        "Generacion": int(gerencia["valorGeneracion"]),
        "Enlace": gerencia["valorEnlace"] if gerencia["valorEnlace"] else None,
        "Pronostico": int(gerencia["valorPronostico"]),
        "Gerencia": gerencia_name,
        "Sistema": obtener_sistema(gerencia_name),
        "FechaCreacion": datetime.now().isoformat(),  # Using ISO format for JSON compatibility
        "FechaModificacion": datetime.now().isoformat(),  # Using ISO format for JSON compatibility
    }

    try:
        response = requests.post(
            f"{API_URL}/demanda", json=data, timeout=20
        )  # Added timeout
        return response
    except requests.exceptions.RequestException as e:
        logger.error("Error sending request to API URL/demanda %s: %s", API_URL, e)
        return None


def obtener_demanda():
    load_dotenv()  # Load environment variables from .env file
    bot_token = os.getenv("TELEGRAM_BOT_MERCADOS_LUX_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    for gerencia_id, gerencia_name in MAPEO_GERENCIAS.items():
        logger.info("Processing gerencia: %s (%s)", gerencia_name, gerencia_id)
        # Datos que se enviarán en la solicitud (formato JSON)
        data = {"gerencia": f"{gerencia_id}"}

        # Encabezados de la solicitud
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
        }

        try:
            # Realizar la solicitud POST con los datos y encabezados proporcionados
            response = requests.post(
                URL, json=data, headers=headers, timeout=20
            )  # Added timeout
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

        except requests.exceptions.RequestException as e:
            logger.error(
                "Error during request to %s for gerencia %s: %s", URL, gerencia_name, e
            )
            continue  # Skip to the next gerencia

        # Obtener la respuesta en formato JSON
        try:
            json_data = response.json()
            # Cargar la cadena JSON asociada a la clave "d" en un formato JSON válido
            d_json = json.loads(json_data["d"])
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(
                "Error parsing JSON response for gerencia %s: %s. Response text: %s",
                gerencia_name,
                e,
                response.text[:500],  # Log first 500 chars
            )
            continue  # Skip to the next gerencia

        # Elimina la hora 24 al final de la lista ya que nunca se actualiza en el día actual
        if d_json and d_json[-1].get("hora") == "24":
            d_json_actualizado: list[Gerencia] = d_json[:-1]
        else:
            d_json_actualizado: list[Gerencia] = d_json

        # Checamos si existe la hora 24 al inicio de la lista ya que esto
        # significa que tenemos datos de la hora 00
        if d_json_actualizado and d_json_actualizado[0].get("hora") == "24":
            logger.info("Hora 00 data found for gerencia %s.", gerencia_name)
            hora_00 = d_json_actualizado[0]
            logger.info("Hora 00 data: %s", hora_00)

            # Check for potentially empty data which might cause int() errors
            if (
                hora_00.get("valorDemanda", "").strip()
                and hora_00.get("valorGeneracion", "").strip()
                and hora_00.get("valorPronostico", "").strip()
            ):
                api_response = enviar_peticion(hora_00, 0, gerencia_name)

                if api_response is not None:
                    # Check the response status
                    if api_response.status_code in CODE_STATUS:
                        logger.info(
                            "API request for Hora 00 (%s) successful (Status: %s).",
                            gerencia_name,
                            api_response.status_code,
                        )
                        # Perform success actions (like percentage check)
                        try:
                            porcentaje, direccion = diferencia_en_porcentaje(
                                int(hora_00["valorPronostico"]),
                                int(hora_00["valorDemanda"]),
                            )
                        except (ValueError, KeyError) as e:
                            logger.error(
                                "Error calculating percentage diff for Hora 00 (%s): %s. Data: %s",
                                gerencia_name,
                                e,
                                hora_00,
                            )

                    elif api_response.status_code == 409:
                        logger.warning(
                            "API request for Hora 00 (%s): Data already exists (Status 409).",
                            gerencia_name,
                        )
                    else:
                        logger.error(
                            "Error sending API request for Hora 00 (%s). Status: %s. Response: %s",
                            gerencia_name,
                            api_response.status_code,
                            api_response.text[:500],  # Log first 500 chars
                        )
                else:
                    logger.error(
                        "Did not receive a response from API for Hora 00 (%s).",
                        gerencia_name,
                    )
            else:
                logger.warning(
                    "Skipping Hora 00 for gerencia %s due to missing values in data: %s",
                    gerencia_name,
                    hora_00,
                )

        # Obtenemos la hora actual para obtener solo la hora que corresponde de la lista
        ahora = datetime.now()
        hora_actual_str = (
            ahora.strftime("%H").lstrip("0") or "0"
        )  # Handle midnight correctly
        hora_actual_int = int(hora_actual_str)
        logger.info(
            "Current hour check: %s for gerencia %s", hora_actual_str, gerencia_name
        )

        # Obtener el valor de la hora actual
        valor_hora_actual = next(
            (
                elemento
                for elemento in d_json_actualizado
                if elemento.get("hora") == hora_actual_str
            ),
            None,
        )

        # Si no se encuentra la hora actual, o la demanda está vacía, se salta esta gerencia
        if (
            valor_hora_actual is None
            or not valor_hora_actual.get("valorDemanda", "").strip()
        ):
            logger.warning(
                "No data available for current hour %s in gerencia %s. Skipping.",
                hora_actual_str,
                gerencia_name,
            )
            continue

        logger.info(
            "Data for current hour %s (%s): %s",
            hora_actual_str,
            gerencia_name,
            valor_hora_actual,
        )

        # Check for potentially empty data which might cause int() errors
        if (
            valor_hora_actual.get("valorDemanda", "").strip()
            and valor_hora_actual.get("valorGeneracion", "").strip()
            and valor_hora_actual.get("valorPronostico", "").strip()
        ):
            api_response = enviar_peticion(
                valor_hora_actual, hora_actual_int, gerencia_name
            )

            if api_response is not None:
                # Check the response status
                if api_response.status_code in CODE_STATUS:
                    logger.info(
                        "API request for Hora %s (%s) successful (Status: %s).",
                        hora_actual_str,
                        gerencia_name,
                        api_response.status_code,
                    )
                    # Perform success actions (like percentage check)
                    try:
                        porcentaje, direccion = diferencia_en_porcentaje(
                            int(valor_hora_actual["valorPronostico"]),
                            int(valor_hora_actual["valorDemanda"]),
                        )
                        if porcentaje >= 10 and direccion != "división por cero":
                            message = (
                                f"ALERTA: Gerencia {gerencia_name} - "
                                f"Demanda {porcentaje}% {direccion} que el pronóstico."
                            )
                            send_telegram_message(bot_token, chat_id, message)
                            logger.warning(
                                "--> ALERT Hora %s (%s): Demand is %s%% %s than forecast.",
                                hora_actual_str,
                                gerencia_name,
                                porcentaje,
                                direccion,
                            )
                    except (ValueError, KeyError) as e:
                        logger.error(
                            "Error calculating percentage diff for Hora %s (%s): %s. Data: %s",
                            hora_actual_str,
                            gerencia_name,
                            e,
                            valor_hora_actual,
                        )

                elif api_response.status_code == 409:
                    logger.warning(
                        "API request for Hora %s (%s): Data already exists (Status 409).",
                        hora_actual_str,
                        gerencia_name,
                    )
                else:
                    logger.error(
                        "Error sending API request for Hora %s (%s). Status: %s. Response: %s",
                        hora_actual_str,
                        gerencia_name,
                        api_response.status_code,
                        api_response.text[:500],  # Log first 500 chars
                    )
            else:
                logger.error(
                    "Did not receive a response from API for Hora %s (%s).",
                    hora_actual_str,
                    gerencia_name,
                )
        else:
            logger.warning(
                "Skipping current hour %s for gerencia %s due to missing values in data: %s",
                hora_actual_str,
                gerencia_name,
                valor_hora_actual,
            )


# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Starting demand data retrieval process...")
    obtener_demanda()
    logger.info("Demand data retrieval process finished.")
# --- End Main Execution ---

```
models.py
```py
# models.py (Revised for 'id' Primary Key)
from flask_sqlalchemy import SQLAlchemy

# Import UniqueConstraint
from sqlalchemy.sql import func
from typing import Dict, Any

db = SQLAlchemy()


class DemandRecord(db.Model):
    __tablename__ = "Demanda"  # Use your actual table name

    # --- Primary Key ---
    # Use a standard auto-incrementing integer ID as the primary key
    id = db.Column(
        db.Integer, primary_key=True
    )  # SQLAlchemy typically handles autoincrement

    # --- Business Key Fields (Not PK, but should be unique together) ---
    FechaOperacion = db.Column(db.Date, nullable=False)
    HoraOperacion = db.Column(db.Integer, nullable=False)  # 0-23
    Gerencia = db.Column(db.String(50), nullable=False)  # Adjust size if needed

    # --- Data fields ---
    Demanda = db.Column(db.Integer, nullable=True)
    Generacion = db.Column(db.Integer, nullable=True)
    Pronostico = db.Column(db.Integer, nullable=True)
    Enlace = db.Column(db.Integer, nullable=True)
    Sistema = db.Column(db.String(10), nullable=False)

    # --- Timestamps ---
    FechaCreacion = db.Column(
        db.DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    FechaModificacion = db.Column(
        db.DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # --- Unique Constraint ---
    # Enforce uniqueness on the combination of the business key fields
    __table_args__ = (
        db.UniqueConstraint(
            "FechaOperacion",
            "HoraOperacion",
            "Gerencia",
            name="uq_demand_record_fecha_hora_gerencia",
        ),
        # Add other constraints or indexes here if needed
    )

    # --- Explicit Constructor ---
    def __init__(self, **kwargs):
        """
        Explicit constructor using SQLAlchemy's recommended approach.
        It accepts keyword arguments matching column names.
        """
        # This calls the parent class's __init__ and handles mapping kwargs
        # to the defined columns. It's the standard way to override.
        super(DemandRecord, self).__init__(**kwargs)

    def __repr__(self):
        # Include id in representation now
        return f"<DemandRecord id={self.id} {self.FechaOperacion} H{self.HoraOperacion} {self.Gerencia}>"

    def data_is_different(self, data_dict: Dict[str, Any]) -> bool:
        """Checks if relevant data fields differ from the incoming dict."""
        if self.Demanda != data_dict.get("Demanda"):
            return True
        if self.Generacion != data_dict.get("Generacion"):
            return True
        if self.Pronostico != data_dict.get("Pronostico"):
            return True
        if self.Enlace != data_dict.get("Enlace"):
            return True
        return False
```
app.py
```py
import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Removed: import traceback - app.logger.exception handles it
import logging  # Import logging
from datetime import datetime, date
from typing import Optional

# Import the DB instance and model from models.py
from models import db, DemandRecord

# --- App Configuration -------------------------------------------------------
from urllib.parse import quote_plus  # NEW – to URL‑encode the ODBC connection string

app = Flask(__name__)
CORS(app)

# -----------------------------------------------------------------------------
# 1. Build a SQL Server URL (unless DATABASE_URL is already set)
#    SQLAlchemy’s canonical form is:
#       mssql+pyodbc://<user>:<password>@<server>:<port>/<database>?driver=<driver>
#    We URL‑encode the driver name because it contains spaces.
# -----------------------------------------------------------------------------
if "DATABASE_URL" in os.environ:
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
else:
    DRIVER = "ODBC Driver 17 for SQL Server"  # or 18, 11, etc.
    USER = os.getenv("DB_USER", "usr_EnergyTrack")  # "
    PASS = os.getenv("DB_PASSWORD", "Z4YH5MfDhNFIVoOcWxeV")
    SERVER = os.getenv(
        "DB_SERVER", "192.168.200.210"
    )  # "hostname,1433" if a custom port
    DBNAME = os.getenv("DB_NAME", "InfoMercado")

    odbc_str = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DBNAME};UID={USER};PWD={PASS}"
    )
    params = quote_plus(odbc_str)  # URL‑encode the whole thing
    app.config["SQLALCHEMY_DATABASE_URI"] = f"mssql+pyodbc:///?odbc_connect={params}"

# app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
#     "DATABASE_URL", "sqlite:///./instance/demand_data.db"
# )
# app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
# app.config["JSON_AS_ASCII"] = False

# --- Basic Logging Configuration ---
# Configure Flask's built-in logger
# In production, you'd likely want more robust configuration
# (e.g., logging to files, setting up handlers and formatters)
# For development, INFO level to console is often sufficient.
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    filename="mercado.log",  # Log to a file
    level=log_level,
)  # Basic config affects root logger
# You can configure app.logger specifically too, but basicConfig often works for simple cases.
# Example: app.logger.setLevel(log_level)
# If not in debug mode, Flask might add its own handlers.

db.init_app(app)

# --- Database Creation ---
with app.app_context():
    # Use app.logger now
    app.logger.info("Creating database tables if they don't exist...")
    try:
        if "sqlite" in app.config["SQLALCHEMY_DATABASE_URI"] and not os.path.exists(
            app.instance_path
        ):
            os.makedirs(app.instance_path, exist_ok=True)
            app.logger.info(f"Using SQLite database in: {app.instance_path}")
        db.create_all()
        app.logger.info("Database tables checked/created successfully.")
    except Exception as e:
        # Use logger.exception to include traceback automatically
        app.logger.exception(
            f"CRITICAL ERROR: Failed to create/check database tables: {e}"
        )


# --- API Endpoints ---
@app.route("/")
def index():
    """Basic index route showing API info"""
    return jsonify(
        {
            "message": "CENACE Data Storage API",
            "status": "running",
            "endpoints": {"submit": "/submit-data (POST)"},
            "timestamp": datetime.now().isoformat(),
        }
    )


@app.route("/demanda", methods=["POST"])
def submit_data_single():
    """
    Receives a SINGLE processed data record via JSON POST request
    and performs database INSERT/UPDATE/CONFLICT logic.
    """
    request_start_time = datetime.now()
    # Use app.logger for logging within the request context
    app.logger.info(
        f"Request received on /submit-data at {request_start_time.isoformat()}"
    )
    outcome = {"status": "unknown", "action": "none", "message": ""}
    http_code = 500  # Default to server error

    # 1. Validate Request
    if not request.is_json:
        app.logger.error("Request content type is not application/json")
        return jsonify(
            {
                "status": "error",
                "message": "Request header 'Content-Type' must be 'application/json'",
            }
        ), 415

    try:
        record_dict = request.get_json()
        if not isinstance(record_dict, dict):
            app.logger.error(
                f"Received data is not a dictionary (Type: {type(record_dict)})."
            )
            return jsonify(
                {
                    "status": "error",
                    "message": "Invalid payload format: body must be a single JSON object.",
                }
            ), 400
    except Exception as e:
        app.logger.error(f"Failed to parse incoming JSON: {e}")
        return jsonify(
            {"status": "error", "message": "Failed to parse request body as JSON."}
        ), 400

    app.logger.info(
        f"Received record content: {record_dict}"
    )  # Log received data (be mindful of sensitive data in production)

    # 2. Extract and Validate Key components
    fecha_op_str = record_dict.get("FechaOperacion")
    hora = record_dict.get("HoraOperacion")
    gerencia = record_dict.get("Gerencia")

    if fecha_op_str is None or hora is None or gerencia is None:
        app.logger.warning(f"Record missing key components: {record_dict}. Rejecting.")
        return jsonify(
            {
                "status": "error",
                "message": "Record missing required key fields (FechaOperacion, HoraOperacion, Gerencia).",
            }
        ), 400

    try:
        fecha_op = date.fromisoformat(fecha_op_str)
        hora = int(hora)
        if not (0 <= hora <= 23):
            raise ValueError("Hour must be between 0 and 23")
    except (ValueError, TypeError) as conv_err:
        app.logger.warning(
            f"Invalid key format in record: {record_dict}. Error: {conv_err}. Rejecting."
        )
        return jsonify(
            {"status": "error", "message": f"Invalid key format: {conv_err}"}
        ), 400

    # 3. Process Record and Interact with Database
    pk = None
    try:
        pk = (fecha_op, hora, gerencia)
        # existing_record: Optional[DemandRecord] = db.session.get(DemandRecord, pk)
        existing_record: Optional[DemandRecord] = DemandRecord.query.filter_by(
            FechaOperacion=fecha_op, HoraOperacion=hora, Gerencia=gerencia
        ).first()

        if existing_record is None:
            # --- INSERT ---
            app.logger.info(f"Record {pk} not found. Attempting INSERT.")

            new_record = DemandRecord(
                FechaOperacion=fecha_op,
                HoraOperacion=hora,
                Gerencia=gerencia,
                Demanda=record_dict.get("Demanda"),
                Generacion=record_dict.get("Generacion"),
                Pronostico=record_dict.get("Pronostico"),
                Enlace=record_dict.get("Enlace"),
                Sistema=record_dict.get("Sistema", "UNK"),
            )
            db.session.add(new_record)
            db.session.commit()
            app.logger.info(f"INSERT successful for {pk}")
            outcome = {
                "status": "success",
                "action": "inserted",
                "message": f"Record {pk} inserted.",
            }
            http_code = 201
        else:
            # --- CHECK FOR UPDATE or CONFLICT ---
            if existing_record.data_is_different(record_dict):
                # --- UPDATE ---
                app.logger.info(
                    f"Record {pk} found. Data is different. Attempting UPDATE."
                )
                existing_record.Demanda = record_dict.get("Demanda")
                existing_record.Generacion = record_dict.get("Generacion")
                existing_record.Pronostico = record_dict.get("Pronostico")
                existing_record.Enlace = record_dict.get("Enlace")
                db.session.commit()
                app.logger.info(f"UPDATE successful for record id {existing_record.id}")
                outcome = {
                    "status": "success",
                    "action": "updated",
                    "message": f"Record id {existing_record.id} ({pk}) updated.",
                }
                http_code = 200
            else:
                # --- CONFLICT ---
                app.logger.info(
                    f"CONFLICT (Identical) for record id {existing_record.id} ({pk}). No changes made."
                )
                outcome = {
                    "status": "conflict",
                    "action": "none",
                    "message": f"Record {pk} already exists with identical data.",
                }
                http_code = 409

    except Exception as db_err:
        # Use logger.exception to log the error *with* traceback
        app.logger.exception(f"Database operation failed for {pk}: {db_err}")
        db.session.rollback()
        outcome = {
            "status": "error",
            "action": "error",
            "message": f"Database error processing record {pk}.",
        }
        http_code = 500

    # 4. Return Final Response
    request_end_time = datetime.now()
    duration = (request_end_time - request_start_time).total_seconds()
    app.logger.info(
        f"Request finished in {duration:.2f} seconds. Outcome: {outcome.get('status', 'error')}"
    )

    return jsonify(outcome), http_code


# --- Run App ---
if __name__ == "__main__":
    # When running directly, make sure logging is set up before app.run
    # (basicConfig call above handles this for simple cases)
    load_dotenv()
    app.run(debug=True, host="0.0.0.0", port=5001)
```
