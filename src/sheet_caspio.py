import gspread
import requests
import time
import os
from oauth2client.service_account import ServiceAccountCredentials

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class GoogleSheetsCaspioTransfer:
    def __init__(self, caspio_config, google_credentials_path):
        self.caspio_config = caspio_config
        self.google_credentials_path = google_credentials_path
        self.caspio_token = None
        self.gc = None

    def authenticate_google_sheets(self):
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.google_credentials_path, scope)
            self.gc = gspread.authorize(credentials)
            print("✓ Google Sheets authentication successful")
            return True
        except Exception as e:
            print(f"✗ Google Sheets authentication failed: {e}")
            return False

    def get_caspio_token(self):
        try:
            account_id = self.caspio_config['account_id'].replace('https://', '').replace('http://', '')
            if account_id.endswith('.caspio.com'):
                account_id = account_id.replace('.caspio.com', '')
            auth_url = f"https://{account_id}.caspio.com/oauth/token"
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.caspio_config['client_id'],
                'client_secret': self.caspio_config['client_secret']
            }
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            response = requests.post(auth_url, data=payload, headers=headers)
            if response.status_code == 200:
                token_data = response.json()
                self.caspio_token = token_data['access_token']
                print("✓ Caspio authentication successful")
                return True
            else:
                print(f"✗ Caspio authentication failed: {response.status_code}")
                print(f"Response: {response.text}")
                return False
        except Exception as e:
            print(f"✗ Caspio authentication error: {e}")
            return False

    def find_tinh_trang_column(self, headers):
        for i, header in enumerate(headers):
            if header.lower().strip() in ['TT Updata', 'TT_updata']:
                return i
        return -1

    def read_google_sheet(self, sheet_url, worksheet_name=None):
        try:
            print(f"📋 Opening Google Sheet: {sheet_url}")
            if 'docs.google.com' in sheet_url:
                sheet = self.gc.open_by_url(sheet_url)
            else:
                sheet = self.gc.open_by_key(sheet_url)
            print(f"✓ Sheet opened successfully: {sheet.title}")
            if worksheet_name:
                try:
                    worksheet = sheet.worksheet(worksheet_name)
                    print(f"✓ Found worksheet: {worksheet.title}")
                except:
                    worksheet = sheet.get_worksheet(0)
                    print(f"❌ Worksheet '{worksheet_name}' not found, using first worksheet: {worksheet.title}")
            else:
                worksheet = sheet.get_worksheet(0)
                print(f"✓ Using first worksheet: {worksheet.title}")
            all_data = worksheet.get_all_values()
            if not all_data:
                print("✗ No data found in the sheet")
                return [], [], -1
            headers = all_data[0] if all_data else []
            tinh_trang_col = 19
            filtered_data = []
            for row_index, row in enumerate(all_data[1:], start=2):
                while len(row) < len(headers):
                    row.append('')
                tinh_trang_value = row[tinh_trang_col] if tinh_trang_col < len(row) else ''
                is_empty = not tinh_trang_value.strip()
                if is_empty:
                    row_data = {
                        'row_number': row_index,
                        'data': row,
                        'original_row': row,
                        'tinh_trang_col': tinh_trang_col
                    }
                    filtered_data.append(row_data)
            return filtered_data, headers, tinh_trang_col
        except Exception as e:
            print(f"✗ Error reading Google Sheet: {e}")
            return [], [], -1

    def send_to_caspio(self, data_rows, field_mappings, headers):
        if not self.caspio_token:
            print("✗ No Caspio token available")
            return []
        account_id = self.caspio_config['account_id'].replace('https://', '').replace('http://', '')
        if account_id.endswith('.caspio.com'):
            account_id = account_id.replace('.caspio.com', '')
        api_url = f"https://{account_id}.caspio.com/rest/v2/tables/{self.caspio_config['table_name']}/records"
        print("DEBUG POST URL:", api_url)
        print("DEBUG TABLE NAME:", self.caspio_config['table_name'])
        headers_http = {
            'Authorization': f'Bearer {self.caspio_token}',
            'Content-Type': 'application/json'
        }
        successful_transfers = []
        print(f"\n🚀 Starting transfer of {len(data_rows)} records...")
        for i, row_data in enumerate(data_rows):
            try:
                record = {}
                for col_index, field_name in field_mappings.items():
                    if col_index < len(row_data['data']):
                        value = row_data['data'][col_index]
                        record[field_name] = value.strip() if value else ''
                response = requests.post(api_url, headers=headers_http, json=record)
                if response.status_code in [200, 201]:
                    response_data = response.json() if response.text else {}
                    successful_transfers.append({
                        'row_number': row_data['row_number'],
                        'record': record,
                        'caspio_response': response_data,
                        'tinh_trang_col': row_data['tinh_trang_col']
                    })
                    print(f"   ✅ SUCCESS: Row {row_data['row_number']} transferred")
                else:
                    print(f"   ❌ FAILED: Row {row_data['row_number']} - Status: {response.status_code}")
                    print(f"   📄 Error details: {response.text}")
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ ERROR transferring row {row_data['row_number']}: {e}")
        return successful_transfers

    def update_google_sheet_status(self, sheet_url, worksheet_name, successful_transfers):
        if not successful_transfers:
            print("ℹ️ No successful transfers to update in Google Sheet")
            return
        try:
            if 'docs.google.com' in sheet_url:
                sheet = self.gc.open_by_url(sheet_url)
            else:
                sheet = self.gc.open_by_key(sheet_url)
            if worksheet_name:
                try:
                    worksheet = sheet.worksheet(worksheet_name)
                except:
                    worksheets = sheet.worksheets()
                    worksheet = worksheets[0] if worksheets else None
            else:
                worksheet = sheet.get_worksheet(0)
            if not worksheet:
                print("❌ No valid worksheet found for updates")
                return
            for transfer in successful_transfers:
                row_number = transfer['row_number']
                tinh_trang_col = transfer['tinh_trang_col']
                if tinh_trang_col != 19:
                    print(f"❌ Error: 'Tình trạng update' column for row {row_number} is not in column T (index 19)")
                    continue
                tinh_trang_col_num = tinh_trang_col + 1
                try:
                    worksheet.update_cell(row_number, tinh_trang_col_num, "Copied")
                    print(f"   ✅ Row {row_number}, Column T set to 'Copied'")
                except Exception as e:
                    print(f"   ❌ Error updating row {row_number}: {e}")
                time.sleep(0.5)
            print(f"✅ Successfully updated {len(successful_transfers)} rows in 'Tình trạng update' column")
        except Exception as e:
            print(f"❌ Error updating Google Sheet: {e}")

    def transfer_data(self, sheet_url, worksheet_name, field_mappings):
        print("🚀 Starting Google Sheets to Caspio transfer...")
        if not self.authenticate_google_sheets():
            print("❌ Transfer aborted due to Google Sheets authentication failure")
            return False
        if not self.get_caspio_token():
            print("❌ Transfer aborted due to Caspio authentication failure")
            return False
        print("\n📊 Reading Google Sheet data...")
        data_rows, headers, tinh_trang_col = self.read_google_sheet(sheet_url, worksheet_name)
        if not data_rows:
            print("ℹ️ No data to transfer (all rows in 'Tình trạng update' column have values or no valid data)")
            return True
        print(f"\n🔄 Transferring {len(data_rows)} rows to Caspio...")
        successful_transfers = self.send_to_caspio(data_rows, field_mappings, headers)
        if successful_transfers:
            print(f"\n📝 Initiating Google Sheet status update for {len(successful_transfers)} successful transfers...")
            self.update_google_sheet_status(sheet_url, worksheet_name, successful_transfers)
        else:
            print("\nℹ️ No successful transfers to update in Google Sheet")
        print("\n📈 FINAL TRANSFER SUMMARY")
        print("="*80)
        print(f"Total rows processed: {len(data_rows)}")
        print(f"Successfully transferred: {len(successful_transfers)}")
        print(f"Failed transfers: {len(data_rows) - len(successful_transfers)}")
        if successful_transfers:
            print(f"\n✅ Transfer completed successfully!")
            print(f"✅ {len(successful_transfers)} rows transferred to Caspio")
            print(f"✅ Google Sheet update attempted for {len(successful_transfers)} rows")
        else:
            print(f"\n❌ No data was successfully transferred")
        return len(successful_transfers) > 0

def main():
    caspio_config = {
        'account_id': os.getenv('CASPIO_ACCOUNT_ID', 'xxxxxxxx'),
        'client_id': os.getenv('CASPIO_CLIENT_ID', '3963d7eb1c12422535eee4626c725879e9cexxxxxxxxxx'),
        'client_secret': os.getenv('CASPIO_CLIENT_SECRET', '175b6ea752db43c98304ebba83db515d0xxxxxxxxxx'),
        'table_name': os.getenv('CASPIO_TABLE_NAME', 'dataQC')
    }
    google_credentials_path = os.path.join(os.path.dirname(__file__), 'google-credentials.json')

    field_mappings = {
        0: 'Advertiser_ID',
        1: 'Advertiser_Name',
        2: 'Campaign_ID',
        3: 'Ad_Group_ID',
        4: 'Ad_ID',
        5: 'Campaign_Name',
        6: 'Ad_Group_Name',
        7: 'Ad_Name',
        8: 'Total_Cost_Spend',
        9: 'Day',
        10: 'Reach',
        11: 'Impressions',
        12: 'Frequency',
        13: 'CPM',
        14: 'Button_Click',
        15: 'CPC',
        16: 'CTR_All',
        17: 'Creative_Page_ID',
        18: 'ChiNhanh'
    }

    sheet_url = os.getenv('SHEET_URL', 'https://docs.google.com/spreadsheets/d/1qYyC6rjohX1S14sYkgJXdoQqcCUEnkJ4N0Lwshi6-9A/edit#gid=xxxxxx')
    worksheet_name = os.getenv('WORKSHEET_NAME', 'Update')

    transfer = GoogleSheetsCaspioTransfer(caspio_config, google_credentials_path)
    transfer.transfer_data(sheet_url, worksheet_name, field_mappings)

if __name__ == "__main__":
    main()
