from datetime import datetime, timedelta
import polars as pl

# Import functions from local modules
from web import get_table_data
from parse import parse_table_data, parse_turbo_status, parse_adv_status, parse_adv_data, parse_rga, rga_wider
from plots import plot_velocity, plot_rga_data

def main():
    base_url = 'https://gems.whoi.edu/GEMS_data/'
    # Current timestamp minus one day
    timestamp = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d%H')

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
    rga_df = pl.DataFrame(parsed_rga)
    print(rga_df.head())

    # Pivot the data to plot pressure by mass vs timestamp
    rga_df_wide = rga_wider(rga_df)
    print(rga_df_wide.head())

    # Parse ADV status data
    status_data = sorted_data.get('S', [])
    parsed_status = parse_adv_status(status_data)
    print("\nParsed ADV status data:") 
    print(pl.DataFrame(parsed_status).head())

    # Parse ADV data
    adv_data = sorted_data.get('D', [])
    parsed_data = parse_adv_data(adv_data)
    print("\nParsed ADV data:") 
    adv_df = pl.DataFrame(parsed_data)
    print(adv_df.head())

    # Plot velocity components from ADV data
    plot_velocity(adv_df)
    

    plot_rga_data(rga_df_wide)


if __name__ == '__main__':
    main()