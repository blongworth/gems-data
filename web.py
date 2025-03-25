import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import polars as pl
import matplotlib.pyplot as plt

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
    
def parse_table_data(table_data):
    """
    Sort table rows by type and prepare for type-specific parsing
    
    Args:
        table_data (list): List of lists containing raw table data
        
    Returns:
        dict: Dictionary of lists containing sorted data by type
    """
    sorted_data = {}
    
    for row in table_data:
        if not row or len(row[0]) < 2:
            continue
            
        # Extract row type from the format "[num]Type:data"
        row_text = row[0]
        type_char = row_text[row_text.find(']') + 1]
        if '!' in row_text:
            data = row_text[row_text.find('!') + 1:]
        else:
            data = row_text[row_text.find(':') + 1:]
        
        if type_char not in sorted_data:
            sorted_data[type_char] = []
        sorted_data[type_char].append(data)
    
    return sorted_data

def parse_turbo_status(data_list):
    """
    Parse turbo pump status data in CSV format
    
    Args:
        data_list (list): List of strings containing turbo pump status data
        
    Returns:
        list: List of dictionaries with parsed values
    """
    parsed_data = []
    
    for data in data_list:
        try:
            fields = data.split(',')
            if len(fields) != 9:
                continue
                
            parsed_data.append({
                'timestamp': datetime.fromisoformat(fields[0].strip()),
                'status': int(fields[1].strip()),
                'speed': int(fields[2].strip()), # Hz
                'power': int(fields[3].strip()), # Watts
                'voltage': int(fields[4].strip()),  # Volts
                'e_temp': int(fields[5].strip()),    # Degrees Celsius
                'p_temp': int(fields[6].strip()),  # Degrees Celsius
                'm_temp': int(fields[7].strip()),  # Degrees Celsius
                'filament': float(fields[8].strip()),  # Amps
            })
        except (ValueError, AttributeError) as e:
            print(f"Error parsing turbo pump status data: {str(e)}")
            continue
            
    return parsed_data

def parse_adv_status(data_list):
    """
    Parse ADV status data in CSV format with multiple fields
    
    Args:
        data_list (list): List of strings containing ADV status data
        
    Returns:
        list: List of dictionaries with parsed values
    """
    parsed_data = []
    
    for data in data_list:
        try:
            fields = data.split(',')
            if len(fields) != 13:
                continue
                
            # Convert individual time fields to a single timestamp
            adv_timestamp = datetime(
                year=int(fields[5].strip()) + 2000,
                month=int(fields[6].strip()),
                day=int(fields[3].strip()),
                hour=int(fields[4].strip()),
                minute=int(fields[1].strip()),
                second=int(fields[2].strip())
            )
            
            parsed_data.append({
                'timestamp': datetime.fromisoformat(fields[0].strip()),
                'adv_timestamp': adv_timestamp,
                'bat': int(fields[7].strip()) * 0.1, # volts
                'soundspeed': int(fields[8].strip()) * 0.1, # m/s
                'heading': int(fields[9].strip()) * 0.1, # degrees (0 if no compass)
                'pitch': int(fields[10].strip()) * 0.1, # degrees
                'roll': int(fields[11].strip()) * 0.1,  # degrees
                'temp': int(fields[12].strip()) * 0.01 # degrees Celsius
            })
        except (ValueError, AttributeError) as e:
            print(f"Error parsing ADV status data: {str(e)},\n data: {data}")
            continue
            
    return parsed_data

def parse_adv_data(data_list):
    """
    Parse ADV data in CSV format
    
    Args:
        data_list (list): List of strings containing ADV data
        
    Returns:
        list: List of dictionaries with parsed values
    """
    parsed_data = []
    
    for data in data_list:
        try:
            fields = data.split(',')
            if len(fields) != 14:
                continue
                
            parsed_data.append({
                'count': int(fields[0].strip()),
                'pressure': int(fields[1].strip()) * 0.001, # decibars
                'u': int(fields[2].strip()) * 0.0001, # m/s
                'v': int(fields[3].strip()) * 0.0001,
                'w': int(fields[4].strip()) * 0.0001,
                'amp1': int(fields[5].strip()), # counts
                'amp2': int(fields[6].strip()),
                'amp3': int(fields[7].strip()),
                'corr1': int(fields[8].strip()), # percent
                'corr2': int(fields[9].strip()),
                'corr3': int(fields[10].strip()),
                'ana_in': int(fields[11].strip()),
                'ana_in2': int(fields[12].strip()),
                'ph_count': int(fields[13].strip())
            })
        except (ValueError, AttributeError) as e:
            print(f"Error parsing ADV data: {str(e)}")
            continue
            
    return parsed_data

def parse_rga(data_list):
    """
    Parse RGA data in format "timestamp, mass, current"
    
    Args:
        data_list (list): List of strings containing R type data
        
    Returns:
        list: List of dictionaries with parsed values
    """
    parsed_data = []
    
    for data in data_list:
        try:
            timestamp, mass, current = data.split(',')
            current_val = int(current.strip()) * 1e-15
            parsed_data.append({
                'timestamp': datetime.fromisoformat(timestamp.strip()),
                'mass': int(mass.strip()),
                'current': current_val,
                'pressure': current_val / 0.081
            })
        except (ValueError, AttributeError) as e:
            print(f"Error parsing R data: {str(e)}")
            continue
            
    return parsed_data

def rga_wider(df):
    # Calculate cycle based on first mass value from each cycle
    first_mass = df.select('mass').row(0)[0]
    df = df.with_columns((pl.col('mass').eq(first_mass).cum_sum()).alias('cycle'))
    
    # Calculate mean timestamp per cycle and round to nearest second
    cycle_means = df.group_by('cycle').agg([
        pl.col('timestamp').mean().dt.round('1s').alias('cycle_ts')
    ])
    df = df.join(cycle_means, on='cycle')
    
    # Select and rename columns
    df = df.select(['cycle_ts', 'mass', 'pressure'])
    
    # Pivot the data wider
    df_wide = df.pivot(
        index='cycle_ts',
        on='mass',
        values='pressure',
        aggregate_function='first'
    ).rename({'cycle_ts': 'timestamp'})
    
    # Rename mass columns to include prefix
    new_names = {str(col): f'mass_{col}' for col in df_wide.columns if col != 'timestamp'}
    return df_wide.rename(new_names)

def plot_rga_data(df_wide):
    """
    Plot RGA mass spectrometry data from a wide-format DataFrame
    
    Args:
        df_wide (polars.DataFrame): DataFrame with timestamp and mass columns
    """
    # Create figure and axis
    plt.figure(figsize=(12, 6))

    # Plot each mass column
    for col in df_wide.columns:
        if col != 'timestamp':
            plt.plot(df_wide['timestamp'], df_wide[col], label=col)

    # Customize plot
    plt.yscale('log')
    plt.xlabel('Time')
    plt.ylabel('Pressure (Torr)')
    plt.title('RGA Mass Spectrometry Data')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Example usage
def main():
    base_url = 'https://gems.whoi.edu/GEMS_data/'
    # Current timestamp minus one day
    timestamp = (datetime.now() - timedelta(days=4)).strftime('%Y%m%d%H')

    # Get data for a specific timestamp
    print(f"Fetching data for timestamp: {timestamp}")
    data = get_table_data(base_url, timestamp=timestamp)
        
    # Parse and sort the data
    print("\nSorting data")
    sorted_data = parse_table_data(data)
    for key, value in sorted_data.items():
        print(f"Type {key}: {len(value)} elements")

    # Parse Turbo data
    turbo_data = sorted_data.get('!', [])
    parsed_turbo = parse_turbo_status(turbo_data)
    print("\nParsed turbo data:") 
    print(pl.DataFrame(parsed_turbo).head())

    # Parse RGA data
    rga_data = sorted_data.get('R', [])
    parsed_rga = parse_rga(rga_data)
    print("\nParsed RGA data:") 
    print(pl.DataFrame(parsed_rga).head())

    # Parse ADV status data
    status_data = sorted_data.get('S', [])
    parsed_status = parse_adv_status(status_data)
    print("\nParsed ADV status data:") 
    print(pl.DataFrame(parsed_status).head())

    # Parse ADV data
    adv_data = sorted_data.get('D', [])
    parsed_data = parse_adv_data(adv_data)
    print("\nParsed ADV data:") 
    print(pl.DataFrame(parsed_data).head())

    # Convert RGA data to DataFrame and get numpy arrays for plotting
    df = pl.DataFrame(parsed_rga)
    print(df.head(11))

    # Pivot the data to plot pressure by mass vs timestamp
    df_wide = rga_wider(df)

    print(df_wide.head())

    plot_rga_data(df_wide)


if __name__ == '__main__':
    main()