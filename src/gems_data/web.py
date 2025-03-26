import requests
from bs4 import BeautifulSoup
from datetime import datetime

def get_table_data(base_url, timestamp=None, table_index=0):
    """
    Extract data from an HTML table on a webpage without headers
    
    Args:
        base_url (str): The base URL of the webpage containing the table
        timestamp (str): Timestamp in YYYYMMDDHH format (optional)
        table_index (int): Index of the table to extract (if multiple tables exist)
        
    Returns:
        list: List of lists containing table row data
    """
    try:
        # Construct URL with timestamp if provided
        url = base_url
        if timestamp:
            # Validate timestamp format
            if len(timestamp) != 10:
                timestamp = datetime.now().strftime('%Y%m%d%H')
            url = f"{base_url}?timestamp={timestamp}"
        
        # Send HTTP request to the URL
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the table
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find_all('table')[table_index]
        
        # Extract row data without headers
        table_data = []
        for row in table.find_all('tr'):
            row_data = [td.text.strip() for td in row.find_all('td')]
            if row_data:
                table_data.append(row_data)
                
        return table_data
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return None