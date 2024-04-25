import os

home_dir = "/home/yunusali/airflow/"

def cleanup_png():
    try:
        directories = [home_dir +"stock_analysis_plots"]

        for directory in directories:
            for file in os.listdir(directory):
                if file.endswith(".png"):
                    os.remove(os.path.join(directory, file))
                    print(f"Removed {file} from {directory}")

    except Exception as e:
        raise Exception(f"Error occurred in cleanup_png task: {e}")

def cleanup_csv():
    try:
        # Define directories to cleanup
        directories = [
            home_dir + "sentiments",
            home_dir + "lagged_sentiments",
            home_dir + "sma"
        ]

        # Loop through each directory and remove CSV files
        for directory in directories:
            for file in os.listdir(directory):
                if file.endswith(".csv"):
                    os.remove(os.path.join(directory, file))
                    print(f"Removed {file} from {directory}")

    except Exception as e:
        raise Exception(f"Error occurred in cleanup_csv task: {e}")
        
if __name__=='__main__':
    cleanup_png()
    cleanup_csv()