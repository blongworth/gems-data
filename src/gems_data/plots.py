# plots 

import matplotlib.pyplot as plt

def plot_velocity(df):
    """
    Plot u, v, w velocity components from ADV data
    
    Args:
        df (polars.DataFrame): DataFrame with u, v, w columns
    """
    plt.figure(figsize=(12, 6))
    
    x = range(len(df))
    plt.plot(x, df['u'], label='u')
    plt.plot(x, df['v'], label='v') 
    plt.plot(x, df['w'], label='w')
    
    plt.xlabel('Row Number')
    plt.ylabel('Velocity (m/s)')
    plt.title('ADV Velocity Components')
    plt.legend()
    plt.grid(True)
    plt.show()

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
