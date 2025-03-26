# parse.py
# Parser functions for raw data

from datetime import datetime
import polars as pl

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
