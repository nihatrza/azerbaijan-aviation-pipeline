import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account

# ==========================================
# KONFİQURASİYA VƏ SABİTLƏR
# ==========================================
GCP_PROJECT_ID = "flight-data-pipeline-512007"
DATASET_ID = "flight_dw"
SERVICE_ACCOUNT_JSON = "/opt/airflow/config/gcp_key.json"

# Aeroport İATA kodları (Bakı və digər yerli hava limanları)
AIRPORT_CODES = ["GYD", "NAJ", "GNJ", "LLK", "FZL"]

# RapidAPI AeroDataBox Açarları (Multi-Key Failover)
API_KEYS = [
    os.getenv("RAPIDAPI_KEY_1", "YOUR_RAPIDAPI_KEY_1"),
    os.getenv("RAPIDAPI_KEY_2", "YOUR_RAPIDAPI_KEY_2")
]

DEFAULT_ARGS = {
    'owner': 'Nihat Rzaguluzada',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ==========================================
# YARDIMÇI FUNKSİYALAR (ETL MƏNTİQİ)
# ==========================================

def get_bigquery_client():
    """BigQuery bağlantısını təhlükəsiz credentials ilə yaradır."""
    if os.path.exists(SERVICE_ACCOUNT_JSON):
        credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON)
        return bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    return bigquery.Client(project=GCP_PROJECT_ID)

def fetch_flights_from_api(airport_code):
    """AeroDataBox API-dən uçuş məlumatlarını çəkir (Failover açar dəstəyi ilə)."""
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/iata/{airport_code}"
    
    # 12 saatlıq pəncərə intervalı
    now = datetime.utcnow()
    from_time = (now - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M")
    to_time = (now + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M")
    
    params = {"withLeg": "true", "direction": "Departure"}
    
    for api_key in API_KEYS:
        headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
        }
        try:
            response = requests.get(f"{url}/{from_time}/{to_time}", headers=headers, params=params, timeout=15)
            if response.status_code == 200:
                return response.json().get("departures", [])
            elif response.status_code in [403, 429]:
                logging.warning(f"API Key {api_key[:5]}... limitə çatdı. Növbəti açara keçilir.")
                continue
        except Exception as e:
            logging.error(f"API sorğusunda xəta oluşdu ({airport_code}): {e}")
            
    return []

def calc_delay(row):
    """Fallback Gecikmə Məntiqi (Qalxış vacibdir, yoxdursa Eniş vaxtı ilə hesablayır)."""
    if pd.notnull(row.get("actual_dep_utc")) and pd.notnull(row.get("sched_dep_utc")):
        diff = (row["actual_dep_utc"] - row["sched_dep_utc"]).total_seconds() / 60.0
        return max(0, int(diff))
    elif pd.notnull(row.get("actual_arr_utc")) and pd.notnull(row.get("sched_arr_utc")):
        diff = (row["actual_arr_utc"] - row["sched_arr_utc"]).total_seconds() / 60.0
        return max(0, int(diff))
    return None

def process_and_transform_data(raw_flights, origin_iata):
    """Xam JSON datanı təmizləyir və relyasion DataFrame strukturuna gətirir."""
    processed = []
    
    for f in raw_flights:
        flight_num = f.get("number")
        if not flight_num:
            continue
            
        airline_name = f.get("airline", {}).get("name", "Unknown Airline")
        dest_iata = f.get("movement", {}).get("airport", {}).get("iata", "N/A")
        dest_city = f.get("movement", {}).get("airport", {}).get("name", "N/A")
        dest_country = f.get("movement", {}).get("airport", {}).get("countryCode", "N/A")
        aircraft_model = f.get("aircraft", {}).get("model", "Unknown Aircraft")
        terminal = f.get("movement", {}).get("terminal", None)
        status = f.get("status", "Unknown")
        
        # UTC vaxt oxunuşları
        sched_dep = f.get("movement", {}).get("scheduledTimeUtc")
        actual_dep = f.get("movement", {}).get("actualTimeUtc")
        sched_arr = f.get("movement", {}).get("scheduledTimeUtc") # API fallback
        actual_arr = f.get("movement", {}).get("actualTimeUtc")

        # Datetime parse etmək
        sched_dep_dt = pd.to_datetime(sched_dep).tz_localize(None) if sched_dep else None
        actual_dep_dt = pd.to_datetime(actual_dep).tz_localize(None) if actual_dep else None
        sched_arr_dt = pd.to_datetime(sched_arr).tz_localize(None) if sched_arr else None
        actual_arr_dt = pd.to_datetime(actual_arr).tz_localize(None) if actual_arr else None
        
        date_key = int(sched_dep_dt.strftime("%Y%m%d")) if sched_dep_dt else int(datetime.utcnow().strftime("%Y%m%d"))
        
        processed.append({
            "flight_number": flight_num,
            "origin_iata": origin_iata,
            "destination_iata": dest_iata,
            "city_name": dest_city,
            "country_code": dest_country,
            "airline_name": airline_name,
            "aircraft_model": aircraft_model,
            "sched_dep_utc": sched_dep_dt,
            "actual_dep_utc": actual_dep_dt,
            "sched_arr_utc": sched_arr_dt,
            "actual_arr_utc": actual_arr_dt,
            "dep_terminal": terminal,
            "status": status,
            "date_key": date_key
        })
        
    df = pd.DataFrame(processed)
    if not df.empty:
        df["delay_minutes"] = df.apply(calc_delay, axis=1).astype("Int64")
    return df

# ==========================================
# İCRA EDİLƏN ANA E T L TASK-I
# ==========================================

def run_flight_etl():
    """Bütün aeroportlar üzrə məlumatları çəkir, təmizləyir və BigQuery-yə yükləyir."""
    client = get_bigquery_client()
    all_dfs = []
    
    for iata in AIRPORT_CODES:
        logging.info(f"Məlumatlar çəkilir: {iata}")
        raw_data = fetch_flights_from_api(iata)
        if raw_data:
            df = process_and_transform_data(raw_data, iata)
            all_dfs.append(df)
            
    if not all_dfs:
        logging.warning("Heç bir yeni uçuş məlumatı tapılmadı.")
        return
        
    final_df = pd.concat(all_dfs, ignore_index=True)
    
    # Deduplikasiya (Eyni reys, gün və kalkış aeroportu üzrə son məlumatı saxla)
    final_df.drop_duplicates(subset=["flight_number", "date_key", "origin_iata"], keep="last", inplace=True)
    
    # BigQuery Staging cədvəlinə yükləmə
    table_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.fact_flights"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.PARQUET,
    )
    
    # DataFrame-i BigQuery-yə yazırıq
    job = client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
    job.result()  # İcranın bitməsini gözləyir
    logging.info(f"Uğurla {len(final_df)} sətir BigQuery {table_ref} cədvəlinə yazıldı.")

# ==========================================
# AIRFLOW DAG TƏRİFİ
# ==========================================

with DAG(
    dag_id='flight_etl_pipeline',
    default_args=DEFAULT_ARGS,
    description='AeroDataBox API to BigQuery Automated Flight Data Pipeline',
    schedule_interval='0 */8 * * *',  # Gündə 3 dəfə (hər 8 saatdan bir)
    catchup=False,
    tags=['aviation', 'bigquery', 'etl', 'flight']
) as dag:

    task_run_etl = PythonOperator(
        task_id='extract_transform_load_flights',
        python_callable=run_flight_etl
    )

    task_run_etl