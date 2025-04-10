demanda.py
```
from typing import TypedDict, Optional
from datetime import datetime
import requests
import json

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

API_URL = "http://192.168.201.7:8081"

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
    Hora: int
    Demanda: int
    Generacion: int
    Enlace: Optional[None]
    Pronostico: int
    Gerencia: str
    Sistema: str
    FechaCreacion: datetime
    FechaModificacion: datetime


def diferencia_en_porcentaje(a: int, b: int) -> tuple[float, str]:
    # Added check for a == 0 to avoid ZeroDivisionError
    if a == 0:
        return (0.0, "división por cero")
    diferencia = ((b - a) / a) * 100
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
    return ""


def enviar_peticion(
    gerencia: Gerencia, hora: int, gerencia_name: str
) -> requests.Response:
    fecha_operacion = datetime.today().strftime("%Y-%m-%d")

    data: DataSchema = {
        "FechaOperacion": fecha_operacion,
        "Hora": hora,
        "Demanda": int(gerencia["valorDemanda"]),
        "Generacion": int(gerencia["valorGeneracion"]),
        "Enlace": gerencia["valorEnlace"] if gerencia["valorEnlace"] else None,
        "Pronostico": int(gerencia["valorPronostico"]),
        "Gerencia": gerencia_name,
        "Sistema": obtener_sistema(gerencia_name),
        "FechaCreacion": datetime.now(),
        "FechaModificacion": datetime.now(),
    }

    return requests.post(API_URL, json=data)


def obtener_demanda():
    for gerencia in MAPEO_GERENCIAS:
        # Datos que se enviarán en la solicitud (formato JSON)
        data = {"gerencia": f"{gerencia}"}

        # Encabezados de la solicitud
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
        }

        # Realizar la solicitud POST con los datos y encabezados proporcionados
        response = requests.post(URL, json=data, headers=headers)
        if response.status_code != 200:
            print(
                f"Error al realizar la solicitud. Código de estado: {response.status_code}"
            )
            return

        # Obtener la respuesta en formato JSON
        json_data = response.json()

        # Cargar la cadena JSON asociada a la clave "d" en un formato JSON válido
        d_json = json.loads(json_data["d"])

        # Elimina la hora 24 al final de la lista ya que nunca se actualiza en
        # el día actual
        d_json_actualizado: list[Gerencia] = d_json[: len(d_json) - 1]
        print("d_json", d_json_actualizado)

        # Checamos si existe la hora 24 al inicio de la lista ya que esto
        # significa que tenemos datos de la hora 00
        if d_json_actualizado[0]["hora"] == "24":
            print("Hora 00 encontrada")
            hora_00 = d_json_actualizado[0]
            print("hora_00", hora_00)
            response = enviar_peticion(hora_00, 0, MAPEO_GERENCIAS[gerencia])
            # Check the response status
            if response.status_code in [200, 201]:  # SUCCESS Codes
                print(
                    f"Petición para Hora 00 enviada con éxito (Status: {response.status_code})."
                )
                # Perform success actions (like percentage check)
                porcentaje, direccion = diferencia_en_porcentaje(
                    int(hora_00["valorDemanda"]), int(hora_00["valorGeneracion"])
                )

                if porcentaje >= 15:
                    print(
                        f"    -> Alerta Hora 00: La demanda es {direccion} en un {porcentaje}% respecto a la generación."
                    )
            elif response.status_code == 409:  # KNOWN CONFLICT
                print(
                    "Petición para Hora 00: Dato ya existe en la base de datos (Status 409)."
                )
            else:  # UNEXPECTED ERROR
                print(
                    f"Error al enviar la petición para Hora 00. Código de estado: {response.status_code}"
                )
                # You might want to print response.text here too for debugging
                # print(f"    -> Response body: {response.text}")

        # Obtenemos la hora actual para obtener solo la hora que corresponde
        # de la lista
        ahora = datetime.now()
        hora_actual = ahora.strftime("%H").lstrip("0")  # H

        # Obtener el valor de la hora actual
        valor_hora_actual = next(
            (
                elemento
                for elemento in d_json_actualizado
                if elemento["hora"] == hora_actual
            ),
            None,
        )

        # Si no se encuentra la hora actual, se retorna y se envia un mensaje
        if valor_hora_actual is None or valor_hora_actual["valorDemanda"] == " ":
            print("No hay datos disponibles para la hora actual")
            continue

        response = enviar_peticion(
            valor_hora_actual, int(hora_actual), MAPEO_GERENCIAS[gerencia]
        )
        # Check the response status
        if response.status_code in [200, 201]:  # SUCCESS Codes
            print(
                f"Petición para Hora {hora_actual} enviada con éxito (Status: {response.status_code})."
            )
            # Perform success actions (like percentage check)
            porcentaje, direccion = diferencia_en_porcentaje(
                int(valor_hora_actual["valorDemanda"]),
                int(valor_hora_actual["valorGeneracion"]),
            )

            print("result", valor_hora_actual)
            if porcentaje >= 15:
                print(
                    f"La demanda es {direccion} en un {porcentaje}% respecto a la generación."
                )
            continue

        elif response.status_code == 409:  # KNOWN CONFLICT
            print(
                f"Petición para Hora {hora_actual}: Dato ya existe en la base de datos (Status 409)."
            )
            continue

        else:  # UNEXPECTED ERROR
            print(
                f"Error al enviar la petición para Hora {hora_actual}. Código de estado: {response.status_code}"
            )
            continue


obtener_demanda()
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
    Hora = db.Column(db.Integer, nullable=False)  # 0-23
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
            "Hora",
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
        return f"<DemandRecord id={self.id} {self.FechaOperacion} H{self.Hora} {self.Gerencia}>"

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

# Removed: import traceback - app.logger.exception handles it
import logging  # Import logging
from datetime import datetime, date
from typing import Optional

# Import the DB instance and model from models.py
from models import db, DemandRecord

# --- App Configuration ---
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get(
    "DATABASE_URL", "sqlite:///./instance/demand_data.db"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["JSON_AS_ASCII"] = False

# --- Basic Logging Configuration ---
# Configure Flask's built-in logger
# In production, you'd likely want more robust configuration
# (e.g., logging to files, setting up handlers and formatters)
# For development, INFO level to console is often sufficient.
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level)  # Basic config affects root logger
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


@app.route("/submit-data", methods=["POST"])
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
    hora = record_dict.get("Hora")
    gerencia = record_dict.get("Gerencia")

    if fecha_op_str is None or hora is None or gerencia is None:
        app.logger.warning(f"Record missing key components: {record_dict}. Rejecting.")
        return jsonify(
            {
                "status": "error",
                "message": "Record missing required key fields (FechaOperacion, Hora, Gerencia).",
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
        existing_record: Optional[DemandRecord] = db.session.get(DemandRecord, pk)

        if existing_record is None:
            # --- INSERT ---
            app.logger.info(f"Record {pk} not found. Attempting INSERT.")
            DemandRecord.FechaCreacion = fecha_op

            new_record = DemandRecord(
                FechaOperacion=fecha_op,
                Hora=hora,
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
    app.run(debug=True, host="0.0.0.0", port=5001)
```
