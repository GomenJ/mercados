import locale
import requests
import os
import re
from bs4 import BeautifulSoup, Tag
import urllib.parse
import logging
import zipfile
import glob
import pandas as pd
import json
from datetime import datetime

# Logging Setup
logging.basicConfig(
    # filename="/var/www/html/mercados/logs/pnd_script.log",
    filename="./logs.txt",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


API_BASE_URL = "http://127.0.0.1:5000"  # Or your server's address/port
API_TARGET_SOURCE_PMLMDA = "data_source_1"
URL = "https://www.cenace.gob.mx/Paginas/SIM/Reportes/PreEnerServConMDA.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:137.0) Gecko/20100101 Firefox/137.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://www.cenace.gob.mx/Paginas/SIM/Reportes/PreEnerServConMDA.aspx",
    "Content-Type": "application/x-www-form-urlencoded",
    "Upgrade-Insecure-Requests": "1",
    "Priority": "u=0, i",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Origin": "https://www.cenace.gob.mx",
    "Connection": "keep-alive",
    "Cookie": "ASP.NET_SessionId=t3gtzhr2evmtdfwdgla1niwh",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

SISTEMAS = ["SIN", "BCA", "BCS"]


def delete_csv_files(directory):
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    for file in csv_files:
        os.remove(file)
        logging.info(f"Deleted: {file}")


def send_dataframe_to_api(df_to_send, api_base_url, target_source):
    """
    Transforms a DataFrame and sends its data as JSON to the specified API endpoint.

    Args:
        df_to_send (pd.DataFrame): The DataFrame containing the data.
                                   Expected columns: Hora (int), Clave (str), PML (float),
                                   Energia (float), Perdidas (float), Congestion (float),
                                   Sistema (str), Fecha (datetime or parseable string).
        api_base_url (str): The base URL of the Flask API (e.g., "http://127.0.0.1:5000").
        target_source (str): The target source key for the API URL path
                             (e.g., "data_source_1", "data_source_3").

    Returns:
        bool: True if the API call resulted in a 2xx status code, False otherwise.
    """
    if not isinstance(df_to_send, pd.DataFrame):
        logging.error("Invalid input: df_to_send must be a Pandas DataFrame.")
        return False

    if df_to_send.empty:
        logging.warning("DataFrame to send is empty. Skipping API call.")
        # Depending on your workflow, an empty DataFrame might be considered a success
        # or failure. Returning False here assumes it's not the intended success state.
        return False

    logging.info(f"Preparing data for API endpoint '{target_source}'...")

    # --- 1. Create a copy to avoid modifying the original DataFrame ---
    df_api = df_to_send.copy()

    # --- 2. Verify required columns exist ---
    required_cols = [
        "Sistema",
        "Fecha",
        "Hora",
        "Clave",
        "PML",
        "Energia",
        "Congestion",
        "Perdidas",
    ]
    missing_cols = [col for col in required_cols if col not in df_api.columns]
    if missing_cols:
        logging.error(
            f"DataFrame is missing required columns for the API: {missing_cols}"
        )
        return False

    # --- 3. Transform 'Fecha' column ---
    # Convert to datetime objects first (handles various string formats), then format to 'YYYY-MM-DD' string.
    try:
        # pd.to_datetime is robust in parsing various date formats
        df_api["Fecha"] = pd.to_datetime(df_api["Fecha"]).dt.strftime("%Y-%m-%d")
        logging.debug("'Fecha' column formatted to YYYY-MM-DD.")
    except Exception as e:
        logging.error(
            f"Failed to parse or format the 'Fecha' column. Ensure it contains valid dates. Error: {e}"
        )
        return False

    # --- 4. Transform 'Hora' column ---
    try:
        # Ensure 'Hora' is numeric, coerce errors, fill NaNs (e.g., with 0), convert to int
        df_api["Hora"] = (
            pd.to_numeric(df_api["Hora"], errors="coerce").fillna(0).astype(int)  # type: ignore
        )

        # Apply formatting: Map hour 24 to '00:00:00'
        # Hour 24 usually represents the interval ending at midnight, which is 00:00 of the next day,
        # but for a daily time value, 00:00:00 is the standard representation.
        df_api["Hora"] = df_api["Hora"].apply(
            lambda h: "00:00:00"
            if h == 24
            else (f"{h:02d}:00:00" if 1 <= h <= 23 else "00:00:00")
            # This handles 1-23 correctly, maps 24 to 00:00:00, and treats other invalid values as 00:00:00
        )
        logging.debug(
            "'Hora' column formatted to HH:00:00 (with 24 mapped to 00:00:00)."
        )
    except Exception as e:
        logging.error(
            f"Failed to process the 'Hora' column. Ensure it contains numeric hour values (1-24). Error: {e}"
        )
        return False

    # --- 5. Select only the columns required by the API ---
    # Ensures no extra columns are sent and sets a specific order (though order doesn't matter in JSON objects)
    try:
        df_api = df_api[required_cols]
    except KeyError:
        # This check should be redundant due to the check in step 2, but added for extra safety
        logging.error(
            "One of the required columns was lost during processing. This should not happen."
        )
        return False

    # --- 6. Convert DataFrame to JSON list of dictionaries ---
    # This is the format the Flask endpoint expects (request.get_json())
    try:
        payload = df_api.to_dict("records")  # type: ignore
        print(payload)
        logging.info(
            f"Successfully converted DataFrame to JSON payload ({len(payload)} records)."
        )
        # Optional: print first record to verify format
        # if payload:
        #    logging.debug(f"Sample payload record: {json.dumps(payload[0], indent=2)}")
    except Exception as e:
        logging.error(f"Failed to convert DataFrame to dictionary list: {e}")
        return False

    # --- 7. Construct the full API URL ---
    if not api_base_url.endswith("/"):
        api_base_url += "/"
    # Ensure target_source doesn't start with /
    target_source_cleaned = target_source.lstrip("/")

    full_api_url = f"{api_base_url}api/v1/mercado/{target_source_cleaned}"
    logging.info(f"Target API URL: {full_api_url}")

    # --- 8. Make the POST request ---
    api_success = False
    try:
        logging.info(f"Sending {len(payload)} records to API...")
        response = requests.post(
            full_api_url,
            json=payload,  # requests handles JSON serialization and headers
            timeout=180,  # Set a timeout (in seconds) for the request
        )

        # Check if the request was successful (status code 2xx)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)

        logging.info(f"API call successful! Status Code: {response.status_code}")
        try:
            # Log the response from the API (if JSON)
            api_result = response.json()
            logging.info("API Response:")
            logging.info(json.dumps(api_result, indent=2))
        except json.JSONDecodeError:
            logging.info("API Response (non-JSON):")
            logging.info(response.text)

        api_success = True  # Mark as success

    except requests.exceptions.ConnectionError as e:
        logging.error(
            f"API Connection Error: Could not connect to {api_base_url}. Is the server running? Details: {e}"
        )
    except requests.exceptions.Timeout:
        logging.error(f"API Error: The request to {full_api_url} timed out.")
    except requests.exceptions.HTTPError as e:
        # Error raised by response.raise_for_status() for 4xx/5xx responses
        logging.error(f"API HTTP Error: Status Code {e.response.status_code}")
        logging.error(f"Reason: {e.response.reason}")
        logging.error(
            f"Response Body: {e.response.text}"
        )  # Show error details from API
    except requests.exceptions.RequestException as e:
        # Catch other potential request errors (e.g., URL issues)
        logging.error(
            f"API Request Error: An error occurred during the request. Details: {e}"
        )
    except Exception as e:
        # Catch any other unexpected errors (e.g., during payload creation if missed earlier)
        logging.error(f"An unexpected error occurred: {e}")

    return api_success


def are_files_different(file1, file2):
    """Compara el contenido de dos archivos y devuelve True si son diferentes."""
    with open(file1, "r") as f1, open(file2, "r") as f2:
        return f1.read() != f2.read()


# Extract dates from the files
def extract_date(file_path):
    with open(file_path, "r") as file:
        for line in file:
            if "Fecha:" in line:  # Check if "Fecha:" is in the line
                date = (
                    line.split("Fecha:")[1].strip().strip('"')
                )  # Remove quotes if present
                return date
    return None


def find_hour_row(file_path):
    with open(file_path, "r") as file:
        for i, line in enumerate(file):
            if "Hora" in line:
                return i
    return -1  # Return -1 if "Hora" is not found


def preprocess_csv(file_path, system_name):
    row_index = find_hour_row(file_path)
    skiprows = row_index if row_index != -1 else 7
    df = pd.read_csv(file_path, delimiter=",", skiprows=skiprows)
    df["Sistema"] = system_name
    df.columns = [col.replace("($/MWh)", "").strip() for col in df.columns]
    return df


def extract_field_value(soup: BeautifulSoup, field_name: str, html_element: str) -> str:
    """
    Extrae el valor de un campo específico dentro de un formulario HTML.

    Parámetros:
    - soup: Objeto BeautifulSoup con la página analizada.
    - field_name: Nombre del campo HTML del que se extraerá el valor.
    - html_element: Tipo de etiqueta HTML donde se encuentra el campo.

    Retorna:
    - Valor del campo codificado en URL para su uso en una solicitud POST.
    """
    element_tag = soup.find(html_element, {"name": field_name})
    element = element_tag.get("value", None) if isinstance(element_tag, Tag) else None
    return str(element)


def get_pml_mda():
    session = requests.session()
    response = session.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, "html.parser")
    view_state = extract_field_value(soup, "__VIEWSTATE", "input")
    period = extract_field_value(soup, "ctl00$ContentPlaceHolder1$txtPeriodo", "input")
    date = extract_field_value(
        soup, "ctl00$ContentPlaceHolder1$hdfStartDateSelected", "input"
    )

    for sistema in SISTEMAS:
        data = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$ddlReporte",
            "ctl00$ContentPlaceHolder1$ddlReporte": "359,322",
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": "29/03/2016",
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$ddlReporte",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state,
            "__VIEWSTATEGENERATOR": "35C9E14B",
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        response = session.post(URL, headers=HEADERS, data=data)
        soup = BeautifulSoup(response.text, "html.parser")

        # Regular expression to capture the VIEWSTATE value
        match = re.search(r"\|hiddenField\|__VIEWSTATE\|([^|]+)", response.text)

        view_state_value = ""

        if match:
            view_state_value = match.group(1)
        else:
            print("VIEWSTATE not found.")

        body = {
            "ctl00$ContentPlaceHolder1$ScriptManager": "ctl00$ContentPlaceHolder1$ScriptManager|ctl00$ContentPlaceHolder1$txtPeriodo",
            "ctl00$ContentPlaceHolder1$ddlReporte": "359,322",
            "ctl00$ContentPlaceHolder1$ddlPeriodicidad": "D",
            "ctl00$ContentPlaceHolder1$ddlSistema": sistema,
            "ctl00$ContentPlaceHolder1$txtPeriodo": period,
            "ctl00$ContentPlaceHolder1$hdfStartDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfEndDateSelected": date,
            "ctl00$ContentPlaceHolder1$hdfMinDateToSelect": "29/01/2016",
            "ctl00$ContentPlaceHolder1$hdfMaxDateToSelect": date,
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$txtPeriodo",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": view_state_value,
            "__VIEWSTATEGENERATOR": "35C9E14B",
            "__VIEWSTATEENCRYPTED": "",
            "__ASYNCPOST": "true",
            "": "",
        }

        response = session.post(URL, headers=HEADERS, data=body)
        soup = BeautifulSoup(response.text, "html.parser")
        # Regular expression to capture the VIEWSTATE value
        match = re.search(r"\|hiddenField\|__VIEWSTATE\|([^|]+)", response.text)

        new_view_state_value = ""
        if match:
            # new_view_state_value = match.group(1)
            new_view_state_value = urllib.parse.quote_plus(match.group(1), safe="")
        else:
            print("VIEWSTATE not found.")

        period_encoded = urllib.parse.quote_plus(str(period), safe="")
        date_encoded = urllib.parse.quote_plus(str(date), safe="")

        marg_data = f"ctl00%24ContentPlaceHolder1%24ddlReporte=359%2C322&ctl00%24ContentPlaceHolder1%24ddlPeriodicidad=D&ctl00%24ContentPlaceHolder1%24ddlSistema={sistema}&ctl00%24ContentPlaceHolder1%24txtPeriodo={period_encoded}&ctl00%24ContentPlaceHolder1%24hdfStartDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfEndDateSelected={date_encoded}&ctl00%24ContentPlaceHolder1%24hdfMinDateToSelect=29%2F01%2F2016&ctl00%24ContentPlaceHolder1%24hdfMaxDateToSelect={date_encoded}&ctl00%24ContentPlaceHolder1%24btnDescargarZIP=Descargar+ZIP&__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={new_view_state_value}&__VIEWSTATEGENERATOR=35C9E14B&__VIEWSTATEENCRYPTED="

        response = session.post(URL, headers=HEADERS, data=marg_data)
        if response.status_code == 200:
            # Verifica el encabezado Content-Disposition
            content_disposition = response.headers.get("Content-Disposition", "")

            if "attachment" in content_disposition and ".zip" in content_disposition:
                # Extrae el nombre real del archivo ZIP si está presente en la cabecera
                # current_directory = os.path.dirname(os.path.abspath(__file__))
                filename = "resultado.zip"
                # file_path = os.path.join(current_directory, filename)
                if "filename=" in content_disposition:
                    filename_match = re.search(
                        r'filename=(?:"([^"]+)"|([^;]+))', content_disposition
                    )
                    if filename_match:
                        filename = filename_match.group(1) or filename_match.group(2)

                # Guarda el archivo ZIP descargado
                with open(filename, "wb") as f:
                    f.write(response.content)
                logging.info(f"Archivo ZIP '{filename}' descargado exitosamente")
                # Extrae el contenido del ZIP
                with zipfile.ZipFile(filename, "r") as zip_ref:
                    zip_ref.extractall()
                # Elimina el ZIP después de extraerlo
                os.remove(filename)
                logging.info(f"Archivo ZIP '{filename}' eliminado exitosamente")
                # Busca archivos con un nombre similar pero diferentes extensiones
                base_name = os.path.splitext(filename)[0]
                csv_file = glob.glob(f"{base_name}.*")
                # Renombra el archivo CSV con el nombre del sistema
                new_filename = f"PML_MDA_{sistema}.csv"
                os.rename(csv_file[0], new_filename)
                logging.info(f"Archivo CSV renombrado a '{new_filename}'")
            else:
                logging.warning(
                    "Advertencia: La respuesta no parece ser un archivo ZIP"
                )
                logging.info(f"Content-Disposition: {content_disposition}")
                # Podrías querer inspeccionar los primeros bytes para confirmar que es un ZIP
                logging.info(f"Primeros bytes: {response.content[:20]}")
        else:
            logging.error(
                f"La solicitud falló con el código de estado: {response.status_code}"
            )

    try:
        # Verifica si todos los archivos son diferentes
        csv_files = [f for f in os.listdir() if f.endswith(".csv")]

        all_different = True
        for i in range(len(csv_files)):
            for j in range(i + 1, len(csv_files)):
                if not are_files_different(csv_files[i], csv_files[j]):
                    # Para windows
                    if os.name == "nt":
                        os.system("cls")
                    # Para mac and linux(here, os.name is 'posix')
                    else:
                        os.system("clear")
                    logging.warning(
                        f"Files {csv_files[i]} and {csv_files[j]} are identical. Skipping merge."
                    )
                    all_different = False
                    break
            if not all_different:
                break

        if all_different:
            logging.info("All files are different. Proceeding to merge.")

            date_sin = extract_date("PML_MDA_SIN.csv")
            date_bca = extract_date("PML_MDA_BCA.csv")
            date_bcs = extract_date("PML_MDA_BCS.csv")

            if date_sin == date_bca == date_bcs:
                logging.info(f"Dates match: {date_sin}. Proceeding to merge.")

                df_sin = preprocess_csv("PML_MDA_SIN.csv", "SIN")
                df_bca = preprocess_csv("PML_MDA_BCA.csv", "BCA")
                df_bcs = preprocess_csv("PML_MDA_BCS.csv", "BCS")

                df_sin_actualizado = pd.concat(
                    [df_sin, df_bca, df_bcs], ignore_index=True
                )

                if os.name == "nt":
                    # Para mac and linux(here, os.name is 'posix')
                    locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")  # on windows
                else:
                    locale.setlocale(locale.LC_TIME, "es_ES.utf8")  # on Linux/macOS

                dt = datetime.strptime(date_sin or "", "%d/%b/%Y")
                df_sin_actualizado["Fecha"] = dt
                df_sin_actualizado = df_sin_actualizado.rename(
                    columns={
                        "Clave del nodo": "Clave",
                        "Precio marginal local": "PML",
                        "Componente de energia": "Energia",
                        "Componente de perdidas": "Perdidas",
                        "Componente de congestion": "Congestion",
                    }
                )
                print(df_sin_actualizado.head())

                # --- Instead of sending all at once, send in batches ---
                BATCH_SIZE = (
                    100  # <<< Choose a batch size (e.g., 100, 250, 500) - Tune this!
                )
                num_records = len(df_sin_actualizado)
                num_batches = (
                    num_records + BATCH_SIZE - 1
                ) // BATCH_SIZE  # Calculate needed batches

                overall_success = True  # Track if all batches succeeded
                records_successfully_sent_count = 0
                logging.info("--- Starting API Upload ---")
                logging.info(f"Total records to send: {num_records}")
                logging.info(f"Batch size: {BATCH_SIZE}")
                logging.info(f"Number of batches: {num_batches}")
                for i in range(num_batches):
                    start_index = i * BATCH_SIZE
                    # end_index calculation ensures we don't go past the end of the DataFrame
                    end_index = min(start_index + BATCH_SIZE, num_records)

                    # Select the batch from the DataFrame using row indices
                    df_batch = df_sin_actualizado.iloc[start_index:end_index]

                    logging.info(
                        f"--- Sending Batch {i + 1}/{num_batches} (Records {start_index + 1}-{end_index}) ---"
                    )

                    if df_batch.empty:
                        logging.warning(f"Batch {i + 1} is empty, skipping.")
                        continue

                    # Call the existing function with the smaller batch DataFrame
                    batch_success = send_dataframe_to_api(
                        df_batch, API_BASE_URL, API_TARGET_SOURCE_PMLMDA
                    )
                    if batch_success:
                        logging.info(f"Batch {i + 1}/{num_batches} sent successfully.")
                        records_successfully_sent_count += len(df_batch)
                    else:
                        logging.error(
                            f"Failed to send Batch {i + 1}/{num_batches}. Check logs above."
                        )
                        overall_success = False
                logging.info("--- API Upload Summary ---")
                logging.info(f"Total records from processed files: {num_records}")
                logging.info(
                    f"Total records successfully sent in batches: {records_successfully_sent_count}"
                )
                if overall_success and records_successfully_sent_count == num_records:
                    logging.info("All batches appear to have been sent successfully.")
                    # Optional: Save the combined CSV locally only if everything was sent
                    try:
                        df_final_to_save = df_sin_actualizado.copy()
                        # Format date as string for CSV saving consistency
                        df_final_to_save["Fecha"] = df_final_to_save[
                            "Fecha"
                        ].dt.strftime("%Y-%m-%d")
                    except Exception as e:
                        logging.error(f"Failed to save combined CSV locally: {e}")
                    return True  # Indicate overall success
                else:
                    logging.error(
                        "One or more batches failed to send, or record counts mismatch."
                    )
                    return False  # Indicate failure
            # delete_csv_files(".")  # Replace "." with the target directory path

        else:
            logging.error("Files are not the same")

    except FileNotFoundError as e:
        logging.error(
            f"Error: {e.filename} no encontrado. Asegúrate de que todos los CSV requeridos estén descargados."
        )

    except Exception as e:
        logging.exception(f"Error inesperado: {e}")


get_pml_mda()
