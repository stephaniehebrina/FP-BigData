import pandas as pd
import joblib
import json
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier

def process_and_update_model():
    try:
        print("Reading current batch data...")
        # Baca batch data terbaru
        df = pd.read_csv('lakehouse-3/streaming-data/current_batch.csv')

        # Preprocessing steps
        print("Preprocessing data...")
        columns_to_drop = ['day', 'month', 'pdays', 'contact', 'previous']
        df_processed = df.drop(columns=columns_to_drop)

        # Tentukan urutan kategori yang diinginkan
        job_map = {
            "student": 2,
            "admin.": 5,
            "management": 10,
            "unemployed": 1,
            "retired": 3,
            "housemaid": 4,
            "entrepreneur": 11,
            "blue-collar": 7,
            "self-employed": 6,
            "technician": 9,
            "services": 8,
            "unknown": 0
        }
        marital_map = {
            "married": 1,
            "divorced": 2,
            "single": 0
        }
        education_map = {
            "unknown": 0,
            "primary": 1,
            "secondary": 2,
            "tertiary": 3
        }
        default_map = {
            "yes": 1,
            "no": 0
        }
        housing_map = {
            "yes": 1,
            "no": 0
        }
        loan_map = {
            "yes": 1,
            "no": 0
        }
        poutcome_map = {
            "unknown": 0,
            "other": 1,
            "failure": 2,
            "success": 3
        }
        y_map = {
            "yes": 1,
            "no": 0
        }

        # Encode columns
        df_processed["job_encoded"] = df_processed["job"].map(job_map)
        df_processed["marital_encoded"] = df_processed["marital"].map(marital_map)
        df_processed["education_encoded"] = df_processed["education"].map(education_map)
        df_processed["default_encoded"] = df_processed["default"].map(default_map)
        df_processed["housing_encoded"] = df_processed["housing"].map(housing_map)
        df_processed["loan_encoded"] = df_processed["loan"].map(loan_map)
        df_processed["poutcome_encoded"] = df_processed["poutcome"].map(poutcome_map)
        df_processed["y_encoded"] = df_processed["y"].map(y_map)

        # Remove original columns
        columns_to_drop = ['job', 'marital', 'education', 'default', 'housing', 'loan', 'poutcome', 'y']
        df_processed = df_processed.drop(columns=columns_to_drop)

        # Prepare features and target
        X = df_processed.drop(columns=['y_encoded'])
        y = df_processed['y_encoded']

        try:
            # Load existing model
            print("Loading existing model...")
            model = joblib.load('RF_91.joblib')
            
            # Update model with new data (partial_fit not available for RandomForest, 
            # so we'll create a new model with updated hyperparameters)
            print("Updating model with new data...")
            model = RandomForestClassifier(
                n_estimators=200,
                max_depth=10,
                min_samples_split=10,
                min_samples_leaf=4,
                max_features='sqrt',
                random_state=42,
                warm_start=True  # This allows updating with new data
            )
            model.fit(X, y)
            
        except FileNotFoundError:
            # If model doesn't exist, create new one
            print("Creating new model...")
            model = RandomForestClassifier(
                n_estimators=200,
                max_depth=10,
                min_samples_split=10,
                min_samples_leaf=4,
                max_features='sqrt',
                random_state=42
            )
            model.fit(X, y)

        # Save updated model
        print("Saving updated model...")
        joblib.dump(model, 'RF_91.joblib')

        # Make predictions on current batch
        print("Making predictions on current batch...")
        predictions = model.predict(X)
        probabilities = model.predict_proba(X)

        # Prepare results
        results = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'predictions': predictions.tolist(),
            'probabilities': probabilities.tolist(),
            'processed_records': len(predictions)
        }

        # Save results for frontend
        with open('prediction_results.json', 'w') as f:
            json.dump(results, f)

        print(f"Processed {len(predictions)} records at {results['timestamp']}")
        print(f"Model updated and saved to RF_91.joblib")
        return results

    except Exception as e:
        print(f"Error processing batch: {str(e)}")
        return None

if __name__ == "__main__":
    process_and_update_model()