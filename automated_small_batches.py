import pandas as pd
import time
import os
import signal
import subprocess
from datetime import datetime

class DataProcessor:
    def __init__(self):
        self.data_dir = 'lakehouse-3/data/bank-data'
        self.model_dir = 'MODELLING'
        self.frontend_process = None
        self.processed_files = set()
            
    def kill_process_on_port(self, port):
        try:
            os.system(f"lsof -ti tcp:{port} | xargs kill -9")
            time.sleep(1)
        except:
            pass
            
    def restart_frontend(self, latest_model):
        try:
            self.kill_process_on_port(5009)
            # Pass the latest model path as an environment variable
            env = os.environ.copy()
            env['LATEST_MODEL'] = latest_model
            cmd = f'python3 frontend/app.py'
            self.frontend_process = subprocess.Popen(cmd.split(), env=env)
            print(f"Frontend updated with model: {latest_model}")
        except Exception as e:
            print(f"Error restarting frontend: {str(e)}")

    def get_latest_model(self):
        model_files = [f for f in os.listdir(self.model_dir) if f.startswith('RF_') and f.endswith('.joblib')]
        if not model_files:
            return None
        return max(model_files, key=lambda x: os.path.getctime(os.path.join(self.model_dir, x)))

    def process_data(self):
        try:
            current_files = {f for f in os.listdir(self.data_dir) if f.endswith('.csv')}
            new_files = current_files - self.processed_files
            
            if new_files:
                current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
                print(f"\n[{current_time}] New data detected!")
                
                # Read and combine all CSV files
                all_data = []
                for filename in current_files:
                    file_path = os.path.join(self.data_dir, filename)
                    df = pd.read_csv(file_path)
                    all_data.append(df)
                
                combined_df = pd.concat(all_data, ignore_index=True)
                print(f"Processing {len(combined_df)} records...")
                
                # Create timestamp for new model
                model_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                os.environ['MODEL_TIMESTAMP'] = model_timestamp
                
                # Run model training
                os.system('python3 MODELLING/model.py')
                
                # Get latest model and update frontend
                latest_model = self.get_latest_model()
                if latest_model:
                    print(f"Using model: {latest_model}")
                    self.restart_frontend(latest_model)
                
                self.processed_files = current_files
                
        except Exception as e:
            print(f"Error: {str(e)}")
    
    def cleanup(self):
        if self.frontend_process:
            self.frontend_process.terminate()
        self.kill_process_on_port(5009)

def main():
    processor = DataProcessor()
    print("Starting monitoring... Press Ctrl+C to stop")
    
    try:
        while True:
            processor.process_data()
            time.sleep(1)
    except KeyboardInterrupt:
        processor.cleanup()

if __name__ == "__main__":
    main()