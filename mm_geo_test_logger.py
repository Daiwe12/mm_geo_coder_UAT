"""
mm_geo_test_logger.py

Bulk testing utility for the MMGeoCoder geocoding module.
Reads addresses from a CSV or Excel file, runs geocoding, logs results and errors,
and outputs a detailed report in CSV or Excel format.

Usage:
    python mm_geo_test_logger.py --input <input_file.csv> --output <output_file.xlsx>

Arguments:
    --input   Path to input CSV/Excel file inside 'data/' folder.
    --output  Path to output file, saved in 'result/' folder.

Functions:
    classify_error: Classifies error types from geocoder results or exceptions.
    get_location:   Runs geocoding for a single address and returns results.
    run_bulk_geocode: Processes all addresses, logs results, and saves the report.
"""
import argparse
import os
import pandas as pd
import time
from datetime import datetime
from mm_geo_coder import MMGeoCoder


def classify_error(result, exception_msg=None):
    """Classify the error type from result or exception"""
    if exception_msg:
        if "Traceback" in exception_msg or "Error" in exception_msg:
            return "Crash"
        return exception_msg.split(":")[0]  # Short message
    elif result is None:
        return "None returned"
    elif not isinstance(result, dict):
        return "Wrong format"
    elif any(result.get(k) is None for k in ['address', 'lat', 'log', 'pcode']):
        return "Missing fields"
    else:
        return "No error"

def get_location(address):
    try:
        if not address or pd.isna(address):
            return None, "Empty input"

        geo_coder = MMGeoCoder(address)
        result = geo_coder.get_geolocation()

        if not result or not isinstance(result, list) or len(result) == 0:
            return None, "No result"

        first = result[0]
        return {
            'address': first.get('address'),
            'pcode': first.get('pcode'),
            'lat': first.get('latitude'),
            'log': first.get('longitude')
        }, None

    except Exception as e:
        return None, str(e)


def run_bulk_geocode(input_path, output_path):
    if not os.path.exists(input_path):
        print(f"❌ Error: File not found — {input_path}")
        return
    
    file_ext = os.path.splitext(input_path)[1].lower()

    if not file_ext:
        raise ValueError("❌ Input file must include an extension (.csv, .xlsx, or .xls) — e.g., addresses.csv")
    elif file_ext == '.csv':
        df = pd.read_csv(input_path, encoding='utf-8-sig')
    elif file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(input_path)
    else:
        raise ValueError(f"❌ Unsupported input file format: {file_ext}. Use .csv, .xlsx, or .xls.")

    df = df.dropna(subset=['address']).drop_duplicates(subset=['address'])

    run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M')}"
    input_file = os.path.basename(input_path)
    output_file = os.path.basename(output_path)
    log_rows = []

    for idx, row in df.iterrows():
        address = row['address']
        #expected_address = row.get('expected_address') if 'expected_address' in row else None
        test_case_id = f"TC_{idx+1:04}"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        start_time = time.time()
        result, error_msg = get_location(address)
        duration = round(time.time() - start_time, 3)

        returned_address = result.get('address') if result else None
        lat = result.get('lat') if result else None
        log = result.get('log') if result else None
        pcode = result.get('pcode') if result else None

        missing_fields = []
        for field in ['address','lat', 'log', 'pcode']:
            if result is None or result.get(field) is None:
                missing_fields.append(field)

        if result and all([lat, log, pcode, returned_address]):
            is_format_ok = "All fields present"
        elif result:
            missing = [k for k in ['lat', 'log', 'pcode', 'address'] if not result.get(k)]
            is_format_ok = f"Missing: {', '.join(missing)}"
        else:
            is_format_ok = "Invalid or empty result"


        error_type = classify_error(result, error_msg)
        if result and all([lat, log, pcode, returned_address]) and not error_msg:
            test_outcome = "Sanity check: Pass"
        elif error_msg:
            test_outcome = "Fail"
        else:
            test_outcome = "⚠️ Partial - Needs Review"

        log_rows.append({
            "Test Case ID": test_case_id,
            "DateTime": timestamp,
            "Duration (sec)": duration,
            "Input Address": address,
            #"Expected Address (Optional)": expected_address,
            "Actual Returned Address": returned_address,
            "Latitude": lat,
            "Longitude": log,
            "PCode": pcode,
            "Result Format OK?": is_format_ok,
            "Missing Fields": ', '.join(missing_fields) if missing_fields else "None",
            "Error Type (if any)": error_type,
            "Exception/Traceback": error_msg,
            "Test Outcome": test_outcome,
            "Notes": ""
        })

    log_df = pd.DataFrame(log_rows)

    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    if output_path.endswith('.xlsx'):
        log_df.to_excel(output_path, index=False)
        print(f"✅ Excel report saved: {output_path}")
    elif output_path.endswith('.csv'):
        log_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"✅ CSV report saved: {output_path}")
    else:
        print("❌ Unsupported output format. Use .csv or .xlsx")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bulk Test mm_geo_coder with Address CSV")
    parser.add_argument('--input', required=True, help="Path to input CSV file inside 'data/' folder")
    parser.add_argument('--output', required=True, help="Path to output file, will be saved in 'result/' folder")
    args = parser.parse_args()
    # Enforce input from ./data/ and output to ./result/
    input_path = os.path.abspath(os.path.join("data", args.input))
    output_path = os.path.abspath(os.path.join("result", args.output))
    print(f"Input Path: {input_path}")
    print(f"Output Path: {output_path}")
    run_bulk_geocode(input_path, output_path)