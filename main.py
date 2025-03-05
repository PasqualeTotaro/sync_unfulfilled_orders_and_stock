import json
import os
from google.cloud import secretmanager
from google.oauth2 import service_account
from googleapiclient.discovery import build
import requests
from datetime import datetime
from pytz import timezone
from typing import NamedTuple

def get_credentials():
    is_cloud = os.getenv('K_SERVICE') is not None
    try:
        client = secretmanager.SecretManagerServiceClient()
    except Exception:
        if is_cloud:
            raise Exception("Failed to authenticate with Secret Manager in Cloud Functions")
        
        # Local development flow
        os.system('gcloud auth application-default set-quota-project analytics-to-sheets-424319')
        os.system('gcloud config set billing/quota_project analytics-to-sheets-424319')
        os.system('gcloud auth application-default login')
        client = secretmanager.SecretManagerServiceClient()

    secret_name = "projects/analytics-to-sheets-424319/secrets/analytics-key/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    secret_content = response.payload.data.decode("UTF-8")
    service_account_info = json.loads(secret_content)
    
    return service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
def get_warehouse_for_country(country_code):
    if country_code == 'GB':
        return 'UK'
    elif country_code == 'UK':
        return 'UK'
    elif country_code == 'US':
        return 'USA'
    elif country_code in ['AU', 'HK', 'SG', 'TW', 'NZ']:
        return 'StorkUp'
    else:
        return 'SPNS'

# Path to your service account key file
KEY_FILE_PATH = 'key.json'
SPREADSHEET_ID = '1S1-GIf-yxrcNg35x0PhuilSgzwyuVHnk2GMR1csCoFY'

# Shopify configuration
ACCESS_TOKEN = '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'
SHOP_NAME = 'oddballism'
API_VERSION = '2024-07'
BASE_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}"
ORDERS_ENDPOINT = f"{BASE_URL}/orders.json"
UK_TIMEZONE = timezone('Europe/London')

shop_headers = {
    'Content-Type': 'application/json',
    'X-Shopify-Access-Token': ACCESS_TOKEN
}

# Sheet names mapping
SHEET_MAPPING = {
    'GB': 'UK',
    'US': 'USA',
    'APAC': 'StorkUp',   # AU, NZ, SG, HK, TW
    'Others': 'SPNS',
    'Partial': 'Partially Fulfilled'
}

def get_last_update_date():
    credentials = get_credentials()
    service = build('sheets', 'v4', credentials=credentials)
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range='Last Update!A2'
    ).execute()
    
    last_update = result.get('values', [[None]])[0][0]
    if last_update:
        return datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
    return None

def check_existing_unfulfilled_orders(stock_manager):
    credentials = get_credentials()
    service = build('sheets', 'v4', credentials=credentials)
    
    sheet_orders = {}
    oldest_date = datetime.now()
    
    for sheet_name in SHEET_MAPPING.values():
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A:E'
        ).execute()
        
        values = result.get('values', [])[1:]
        for row in values:
            order_date = datetime.strptime(row[4], '%Y-%m-%d')
            oldest_date = min(oldest_date, order_date)
            order_number = str(row[0])  # Get order number from first column
            sheet_orders[order_number] = {
                'order_number': order_number,
                'skus': row[1],
                'country': row[2],
                'email': row[3],
                'order_date': row[4]
            }    

    params = {
        'status': 'open',
        'created_at_min': oldest_date.strftime('%Y-%m-%d'),
        'limit': 250
    }
    
    current_unfulfilled = set()
    while True:
        response = requests.get(ORDERS_ENDPOINT, headers=shop_headers, params=params)
        orders = response.json().get('orders', [])
        
        for order in orders:
            current_unfulfilled.add(str(order['order_number']))
        
        link_header = response.headers.get('Link')
        if not link_header or 'next' not in link_header:
            break
            
        next_url = [link.split(';')[0].strip('<>') for link in link_header.split(',') 
                   if 'next' in link][0]
        params = dict(param.split('=') for param in next_url.split('?')[1].split('&'))
    
    fulfilled_orders = []
    for order_num, data in sheet_orders.items():
        if order_num not in current_unfulfilled:
            fulfilled_orders.append(data)
    
    stock_manager.process_fulfilled_orders(fulfilled_orders)
    
    orders_by_sheet = {sheet: [] for sheet in SHEET_MAPPING.values()}

    for order in fulfilled_orders:
        country_code = order['country']
        sheet_name = SHEET_MAPPING['GB'] if country_code == 'GB' else \
                    SHEET_MAPPING['US'] if country_code == 'US' else \
                    SHEET_MAPPING['APAC'] if country_code in ['AU', 'HK', 'SG', 'TW', 'NZ'] else \
                    SHEET_MAPPING['Others']
        orders_by_sheet[sheet_name].append(order['order_number'])

    # Remove orders in batches per sheet
    for sheet_name, order_numbers in orders_by_sheet.items():
        if order_numbers:  # Only process sheets with orders to remove
            remove_order_from_sheet(service, sheet_name, order_numbers)
        
def get_orders(last_check_date):
    params = {
        'created_at_min': last_check_date.strftime('%Y-%m-%d %H:%M:%S'),
        "status": "any",
        "limit": 250,
        "fields": "order_number,fulfillment_status,line_items,email,created_at,shipping_address"
    }
    
    unfulfilled_orders = {
        'GB': [],
        'US': [],
        'APAC': [],
        'Others': [],
        'Partial': []
    }
    
    fulfilled_orders = []
    apac_countries = ['AU', 'HK', 'SG', 'TW', 'NZ']
    
    while True:
        response = requests.get(ORDERS_ENDPOINT, headers=shop_headers, params=params)
        orders = response.json().get('orders', [])
        
        for order in orders:
            if not order.get('shipping_address'):
                continue
            
            skus = []
            if order['fulfillment_status'] == 'partial':
                for line_item in order['line_items']:
                    if not line_item.get('fulfillment_status'):
                        skus.append(line_item['sku'])
            else:
                skus = [item['sku'] for item in order['line_items']]
            
            order_data = {
                'order_number': order['order_number'],
                'skus': ','.join(skus),
                'country': order['shipping_address']['country_code'],
                'email': order['email'],
                'order_date': order['created_at'].split('T')[0]
            }
            
            if order['fulfillment_status'] == 'fulfilled':
                fulfilled_orders.append(order_data)
                continue
                
            if order['fulfillment_status'] == 'partial':
                unfulfilled_orders['Partial'].append(order_data)
                continue
                
            country_code = order['shipping_address']['country_code']
            if country_code == 'GB':
                unfulfilled_orders['GB'].append(order_data)
            elif country_code == 'US':
                unfulfilled_orders['US'].append(order_data)
            elif country_code in apac_countries:
                unfulfilled_orders['APAC'].append(order_data)
            else:
                unfulfilled_orders['Others'].append(order_data)
    
        link_header = response.headers.get('Link')
        if not link_header or 'next' not in link_header:
            break
            
        next_url = [link.split(';')[0].strip('<>') for link in link_header.split(',') 
                   if 'next' in link][0]
        params = dict(param.split('=') for param in next_url.split('?')[1].split('&'))
    
    return unfulfilled_orders, fulfilled_orders

def update_sheets(orders):
    credentials = get_credentials()
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    
    for region, order_list in orders.items():
        sheet_name = SHEET_MAPPING[region]
        
        # Get existing orders
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A2:F'
        ).execute()
        
        existing_orders = set()
        values = result.get('values', [])
        if len(values) > 1:
            existing_orders = set(row[0] for row in values[1:])
        
        # Filter out duplicates and prepare new values
        new_orders = [
            [int(order['order_number']), order['skus'], order['country'], 
             order['email'], order['order_date']]
            for order in order_list 
            if str(order['order_number']) not in existing_orders
        ]
        
        if new_orders:
            # Convert existing order numbers to integers and combine with new orders
            existing_values = []
            if len(values) > 1:
                existing_values = [[int(row[0])] + row[1:] for row in values[1:]]
            
            all_values = new_orders + existing_values
            
            # Clear sheet (except header) and update with new content
            service.spreadsheets().values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{sheet_name}!A2:F'
            ).execute()
            
            body = {'values': all_values}
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{sheet_name}!A2',
                valueInputOption='RAW',
                body=body
            ).execute()
            
def remove_order_from_sheet(service, sheet_name, orders_to_remove):
    # Get current sheet data
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{sheet_name}!A2:F'
    ).execute()
    
    values = result.get('values', [])
    new_values = [values[0]]  # Keep header
    
    # Convert orders_to_remove to set of integers for O(1) lookup
    orders_set = set(int(order) for order in orders_to_remove)
    
    # Filter out the orders that need to be removed
    for row in values[1:]:
        if int(row[0]) not in orders_set:
            row[0] = int(row[0])  # Convert to integer
            new_values.append(row)
    
    # Clear everything except header
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{sheet_name}!A2:F'
    ).execute()
    
    # Update with filtered values
    if len(new_values) > 1:  # Only update if there are rows besides header
        body = {'values': new_values[1:]}  # Exclude header from update
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A2',  # Start from A2 to preserve header
            valueInputOption='RAW',
            body=body
        ).execute()         

UK_TIMEZONE = timezone('Europe/London')
def save_last_update_date():
    credentials = get_credentials()
    service = build('sheets', 'v4', credentials=credentials)
    current_date = datetime.now(UK_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
    
    values = [['Last Update'], [current_date]]
    body = {'values': values}
    
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range='Last Update!A1',
        valueInputOption='RAW',
        body=body
    ).execute()

STOCK_SPREADSHEET_ID = '1R3oSmt4k8pfe-h6teNoQh44BZLYsCCIrJzVqHlcIqtg'
STOCK_SHEET_NAME = 'Stock'
class StockManager:
    def __init__(self):
        self.stock_data = {}
        self.load_current_stock()

    def load_current_stock(self):
        credentials = get_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        
        result = service.spreadsheets().values().get(
            spreadsheetId=STOCK_SPREADSHEET_ID,
            range=f'{STOCK_SHEET_NAME}!A:E'
        ).execute()
        
        values = result.get('values', [])
        headers = values[0][1:]  # Warehouses
        
        for row in values[1:]:
            sku = row[0]
            stock = {headers[i]: int(row[i+1]) if row[i+1] else 0 
                    for i in range(len(headers))}
            self.stock_data[sku] = stock

    def update_stock(self, skus, warehouse):
        for sku in skus:
            if sku in self.stock_data:
                self.stock_data[sku][warehouse] -= 1
    
    def process_fulfilled_orders_from_sheet(self, orders):
        for order in orders:
            warehouse = get_warehouse_for_country(order['sheet'])
            if warehouse:
                for sku in order['skus']:
                    individual_skus = sku.split('+')
                    for sku in individual_skus:
                        if sku in self.stock_data:
                            self.stock_data[sku][warehouse] -= 1

    def process_fulfilled_orders(self, orders):
        for order in orders:
            warehouse = get_warehouse_for_country(order['country'])
            if warehouse:
                for sku_bundle in order['skus'].split(','):
                    individual_skus = sku_bundle.split('+')
                    for sku in individual_skus:
                        if sku in self.stock_data:
                            self.stock_data[sku][warehouse] -= 1
                            
    def commit_changes(self):
        values = [['SKU', 'UK', 'USA', 'SPNS', 'Storkup']]
        for sku, warehouses in self.stock_data.items():
            row = [sku]
            row.extend([warehouses[w] for w in ['UK', 'USA', 'SPNS', 'Storkup']])
            values.append(row)

        credentials = get_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        
        body = {'values': values}
        service.spreadsheets().values().update(
            spreadsheetId=STOCK_SPREADSHEET_ID,
            range=f'{STOCK_SHEET_NAME}!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
class APIStatus(NamedTuple):
    status_code: int
    message: str
def main(request) -> APIStatus:
    try:
        stock_manager = StockManager()
        last_update = get_last_update_date()
        check_existing_unfulfilled_orders(stock_manager)
        unfulfilled_orders, fulfilled_orders = get_orders(last_update)
        stock_manager.process_fulfilled_orders(fulfilled_orders)
        update_sheets(unfulfilled_orders)
        save_last_update_date()
        stock_manager.commit_changes()

        return {
            'statusCode': 200,
            'message': 'Orders successfully updated in Google Sheets'
        }
        
    except Exception as e:
        return {
            'statusCode': 400,
            'message': str(e)
        }
    
if __name__ == "__main__":
    main(None)