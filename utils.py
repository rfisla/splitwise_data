import yaml
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from splitwise import Splitwise

class GoogleDrive:
    def __init__(self):
        self.config = self._load_config('config.yml')
        self.folder_id = self.config['drive_folder']
        self.sheet_name = self.config['spreadsheet_name']

        self.key_path = 'service_account.json'
        self.scopes  = ['https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive',
                        'https://spreadsheets.google.com/feeds']
        self.credentials = self._get_sa_credentials()

    @staticmethod
    def _load_config(yaml_path):
        with open(yaml_path, 'r') as stream:
            try:
                config_file = yaml.safe_load(stream)
            except Exception as e:
                print(f"Error reading config file: {e}")
        return config_file.get('GoogleDrive_config')

    def _get_sa_credentials(self):
        credentials = Credentials.from_service_account_file(self.key_path, scopes= self.scopes)
        return credentials

    def upload_spreadsheet(self, df):
        gd = gspread.authorize(self.credentials)
        try:
            # Try to create a new spreadsheet
            sh = gd.create(self.sheet_name, folder_id=self.folder_id)
        except:
            # If the spreadsheet already exists, open it and clear its contents
            sh = gd.open(self.sheet_name)
            sh.values_clear('Hoja 1')
        worksheet = sh.get_worksheet(0)
        worksheet.insert_row(df.columns.tolist(), 1)
        worksheet.insert_rows(df.values.tolist(), 2)

    def download_spreadsheet(self, sheet_key, sheet_name):
        gc = gspread.authorize(self.credentials)
        sh = gc.open_by_key(sheet_key)
        worksheet = sh.worksheet(sheet_name)
        return worksheet


class SplitwiseClass:
    def __init__(self):
        self.auth = self._load_config('config.yml')

        self.consumer_key = self.auth['consumer_key']
        self.consumer_secret = self.auth['consumer_secret']
        self.api_key = self.auth['api_key']
        self.target_group_id = '29489850'
        self.splitwise = self._get_splitwise_object()
        self.target_group_expenses = self._get_target_group_expenses()

    @staticmethod
    def _load_config(yaml_path):
        with open(yaml_path, 'r') as stream:
            try:
                config_file = yaml.safe_load(stream)
            except Exception as e:
                print(f"Error reading config file: {e}")
        return config_file.get('API_keys')

    def _get_splitwise_object(self):
        splitwise = Splitwise(self.consumer_key, self.consumer_secret)
        splitwise.api_key = self.api_key
        return splitwise

    def _get_target_group_expenses(self):
        group_expenses = self.splitwise.getExpenses(limit=100000, group_id=self.target_group_id)
        return group_expenses

    def _extract_info_from_expenses_object(self):
        ids = list(map(lambda expense: expense.getId(), self.target_group_expenses))
        group_ids = list(map(lambda expense: expense.getGroupId(), self.target_group_expenses))
        costs = list(map(lambda expense: expense.getCost(), self.target_group_expenses))
        effective_date = list(map(lambda expense: expense.getDate(), self.target_group_expenses))
        description = list(map(lambda expense: expense.getDescription(), self.target_group_expenses))
        category = list(map(lambda expense: expense.getCategory().getName(), self.target_group_expenses))

        return ids, group_ids, costs, effective_date, description, category

    def create_dataframe(self):
        ids_list, group_ids_list, costs_list, effective_dates_list, \
            description_list, categories_list = self._extract_info_from_expenses_object()
        df = {
            'expense_id': ids_list,
            'group_id': group_ids_list,
            'cost': costs_list,
            'effective_date': effective_dates_list,
            'description': description_list,
            'category': categories_list
        }

        df = pd.DataFrame(df, dtype=str)
        return df

    def _extract_info_from_expenses_object(self):
        ids = list(map(lambda expense: expense.getId(), self.target_group_expenses))
        group_ids = list(map(lambda expense: expense.getGroupId(), self.target_group_expenses))
        costs = list(map(lambda expense: expense.getCost(), self.target_group_expenses))
        effective_date = list(map(lambda expense: expense.getDate(), self.target_group_expenses))
        description = list(map(lambda expense: expense.getDescription(), self.target_group_expenses))
        category = list(map(lambda expense: expense.getCategory().getName(), self.target_group_expenses))

        return ids, group_ids, costs, effective_date, description, category
