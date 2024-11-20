class SaveData:
  @staticmethod
  def save_data_to_file(data, file_name, file_format='csv'):
      """
      Save data to a file in CSV or Excel format.
      Args:
          data (pd.DataFrame): Data to save.
          file_name (str): File name without extension.
          file_format (str): Format to save, either 'csv' or 'xlsx'.
      """
      if file_format == 'csv':
          data.to_csv(f'{file_name}.csv', index=False)
      elif file_format == 'xlsx':
          data.to_excel(f'{file_name}.xlsx', index=False)
      else:
          raise ValueError("Unsupported file format. Use 'csv' or 'xlsx'.")
