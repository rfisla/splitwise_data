from utils import  SplitwiseClass, GoogleDrive

if __name__ == '__main__':
    splitwise = SplitwiseClass()
    splitwise_df = splitwise.create_dataframe()

    gdrive = GoogleDrive()
    gdrive.upload_spreadsheet(splitwise_df)





