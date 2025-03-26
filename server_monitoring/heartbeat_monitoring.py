import requests
import numpy as np
import smtplib
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
import logging
from email.mime.text import MIMEText
from sklearn.ensemble import IsolationForest
from sklearn.model_selection import GridSearchCV

# Configure logging
logging.basicConfig(filename="server_monitor.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Dynatrace API Configuration
DYNATRACE_API_URL = "https://your-dynatrace-api.com/v1/metrics"
DYNATRACE_API_KEY = "your_api_key"

# Email Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_SENDER = "your_email@gmail.com"
EMAIL_PASSWORD = "your_password"
EMAIL_RECEIVER = "receiver_email@gmail.com"

# List of 25 servers to monitor
SERVERS = ["server1", "server2", "server3", ..., "server25"]

# Fetch server metrics from Dynatrace API
def fetch_metrics():
    headers = {"Authorization": f"Api-Token {DYNATRACE_API_KEY}"}
    metrics_data = []
    for server in SERVERS:
        response = requests.get(f"{DYNATRACE_API_URL}/{server}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            cpu_usage = data["cpuUsage"]
            disk_usage = data["diskUsage"]
            metrics_data.append([server, cpu_usage, disk_usage])
        else:
            logging.warning(f"Failed to fetch data for {server}")
    return pd.DataFrame(metrics_data, columns=["Server", "CPU Usage", "Disk Usage"])

# Train anomaly detection model with hyperparameter tuning using real server data
def train_model():
    historical_data = fetch_metrics()
    if historical_data.empty:
        logging.error("No historical data available for training.")
        return None
    
    X_train = historical_data[["CPU Usage", "Disk Usage"]].values
    
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_samples': ['auto', 100, 500],
        'contamination': [0.01, 0.05, 0.1]
    }
    
    model = IsolationForest()
    grid_search = GridSearchCV(model, param_grid, scoring='accuracy', cv=3)
    grid_search.fit(X_train)
    best_model = grid_search.best_estimator_
    logging.info(f"Best model parameters: {grid_search.best_params_}")
    
    return best_model

# Calculate dynamic thresholds
def calculate_dynamic_thresholds(df):
    df['CPU Threshold'] = df['CPU Usage'].rolling(window=5, min_periods=1).quantile(0.95)
    df['Disk Threshold'] = df['Disk Usage'].rolling(window=5, min_periods=1).quantile(0.95)
    return df

# Send email alert
def send_email_alert(message):
    msg = MIMEText(message)
    msg["Subject"] = "Server Health Anomaly Alert!"
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER
    
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()
        logging.info("Email alert sent!")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

# Save results to CSV
def save_results(df):
    df.to_csv("server_anomalies.csv", index=False)
    logging.info("Results saved to server_anomalies.csv")

# Streamlit Dashboard
def streamlit_dashboard(df):
    st.title("Server Anomaly Detection Dashboard")
    st.write("Live monitoring of server health metrics.")
    
    # Display server metrics
    st.dataframe(df)
    
    # Plot graphs
    st.line_chart(df.set_index("Server")["CPU Usage"])
    st.line_chart(df.set_index("Server")["Disk Usage"])
    
    # Highlight anomalies
    anomalies = df[df["Anomaly"] == -1]
    if not anomalies.empty:
        st.error("Anomalies detected in:")
        st.write(anomalies)

# Main monitoring loop
def monitor():
    model = train_model()
    if model is None:
        return
    
    while True:
        df = fetch_metrics()
        if not df.empty:
            df = calculate_dynamic_thresholds(df)
            predictions = model.predict(df[["CPU Usage", "Disk Usage"]])
            df["Anomaly"] = predictions
            
            anomalies = df[(df["Anomaly"] == -1) | 
                           (df["CPU Usage"] > df["CPU Threshold"]) | 
                           (df["Disk Usage"] > df["Disk Threshold"])]
            
            if not anomalies.empty:
                send_email_alert(f"Warning! Anomalies detected in: {anomalies['Server'].tolist()}")
                save_results(anomalies)
                
            # Run Streamlit Dashboard
            streamlit_dashboard(df)
        logging.info("Monitoring cycle completed. Sleeping for 10 minutes...")
        time.sleep(600)  # Check every 10 minutes

if __name__ == "__main__":
    monitor()
