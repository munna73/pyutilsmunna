import logging
import os
import requests
from configparser import ConfigParser
from oracle_connector import OracleConnector  # Your existing class

# ---------- Setup ---------- #

CONFIG_PATH = "config.ini"
LOG_FILE = "service_verification.log"

PRIORITY_LIST = [
    "ROSS",
    "MICHEALS",
    "TJMAX",
    "NRACK",
    "SOFF",
    "MACY",
    "BURLINGTON",
    "TARGET",
    "WALMART",
    "KOHLS",
    "NORDSTROM",
    "SEARS",
    "COSTCO",
    "SAMCLUB",
    "JCPENNEY",
    "OLDNAVY",
    "GAP",
    "BANANAREPUBLIC",
    "PRIMARK",
    "UNIQLO",
]

# ---------- Logging ---------- #

logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------- Service Callers ---------- #


def call_mock_service(item_number):
    """
    Simulates a response from the web service.
    """
    mock_service_data = {1012: "ABC123", 2020: "DEF456", 3033: "GHI789"}
    logger.info("[SIMULATION] Using mock service.")
    return mock_service_data.get(item_number)


def call_actual_service(item_number, config_path=CONFIG_PATH):
    """
    Calls the actual web service with item_number.
    """
    try:
        config = ConfigParser()
        config.read(config_path)
        base_url = config.get("service", "base_url")
        url = base_url.format(item_number=item_number)

        logger.info(f"Calling actual service at: {url}")
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            result = response.json()
            return result.get("s3_key_id")
        else:
            logger.error(f"Service returned non-200 status: {response.status_code}")
            return None
    except Exception as e:
        logger.exception("Failed to call actual service")
        return None


# ---------- Priority Resolver ---------- #


def get_expected_s3_key_new(df, priority_list, b_id=None):
    """
    Selects the s3_bucket_id from a DataFrame based on store priority and latest enc_dt.
    
    Parameters:
    - df (pd.DataFrame): Input DataFrame with columns ['std_img', 'enc_dt', 'b_id', 's3_bucket_id'].
    - priority_list (list): Ordered list of store names (e.g., ['ROSS', 'TJMAX', 'NRACK']).
    - b_id (optional): If provided, filter the DataFrame to only include rows with this b_id.

    Returns:
    - str or None: s3_bucket_id of the matched row, or None if no match.
    """
    if df.empty:
        logging.warning("DataFrame is empty.")
        return None

    # Normalize store names to upper case for matching
    df = df.copy()
    df['std_img'] = df['std_img'].astype(str).str.upper()

    # Optional: filter by b_id
    if b_id is not None:
        df = df[df['b_id'] == b_id]
        if df.empty:
            logging.warning(f"No rows found for b_id = {b_id}")
            return None

    # Ensure enc_dt is in datetime format
    df['enc_dt'] = pd.to_datetime(df['enc_dt'], errors='coerce')

    # Drop rows with invalid dates
    df = df.dropna(subset=['enc_dt'])

    for store in priority_list:
        store_upper = store.upper()
        filtered = df[df['std_img'] == store_upper]
        if not filtered.empty:
            # Sort by enc_dt descending to get latest
            latest_row = filtered.sort_values(by='enc_dt', ascending=False).iloc[0]
            logging.info(f"Matched store: {store}, Row: {latest_row.to_dict()}")
            return latest_row['s3_bucket_id']

    logging.warning("No matching priority store found.")
    return None

def get_expected_s3_key(df, priority_list):
    df['std_img'] = df['std_img'].str.upper()

    for _, row in df.iterrows():
        store = row['std_img']
        if store in [p.upper() for p in priority_list]:
            logging.info(f"Matched priority store: {store}, Full row: {row.to_dict()}")
            return row['s3_bucket_id']

    logging.warning("No matching priority store found.")
    return None


# ---------- Main Verifier ---------- #


def verify_service(item_number, use_mock=True, config_path=CONFIG_PATH):
    """
    Verifies the service response for a given item_number.
    """
    try:
        logger.info(f"🔍 Verifying item_number: {item_number}")

        # Load query from config and replace placeholder
        config = ConfigParser()
        config.read(config_path)
        raw_query = config.get("queries", "s3_key_query")
        final_query = raw_query.format(item_number=item_number)

        # Connect to Oracle and get expected result set
        db = OracleConnector(config_path)
        df = db.run_query(final_query)

        if df.empty:
            logger.warning(f"[SKIP] No records found in DB for item {item_number}")
            return

        # Determine expected s3_key_id from priority logic
        expected_s3 = get_expected_s3_key(df, PRIORITY_LIST)

        # Call either mock or actual service
        if use_mock:
            actual_s3 = call_mock_service(item_number)
        else:
            actual_s3 = call_actual_service(item_number, config_path)

        logger.info(f"Expected s3_key_id: {expected_s3}")
        logger.info(f"Actual s3_key_id: {actual_s3}")

        # Compare
        if actual_s3 == expected_s3:
            logger.info(f"[PASS] Item {item_number}: Correct s3_key_id returned.")
        else:
            logger.error(
                f"[FAIL] Item {item_number}: Expected '{expected_s3}', got '{actual_s3}'"
            )

    except Exception as e:
        logger.exception(f"[ERROR] Unexpected error for item {item_number}: {e}")


# ---------- Run Tests ---------- #

if __name__ == "__main__":
    # Test multiple item numbers
    item_numbers_to_test = [1012, 2020, 3033, 4044]

    # True = simulate, False = call real service
    use_mock_service = False

    for item_number in item_numbers_to_test:
        verify_service(item_number, use_mock=use_mock_service)
