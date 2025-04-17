import pandas as pd
import numpy as np
from xgboost import XGBRegressor
import matplotlib.pyplot as plt

# Load CSV
df = pd.read_csv("data/monthly_sales.csv")
df["year_month"] = pd.to_datetime(df["year_month"])
df = df.sort_values("year_month").set_index("year_month")

# Add time index
df["t"] = np.arange(len(df))

# Prepare training set
X = df[["t"]]
y = df["monthly_total"]

# Train XGBoost model (optimized)
model = XGBRegressor(objective="reg:squarederror", n_estimators=25, verbosity=0)
model.fit(X, y)

# Forecast next 3 months
future_periods = 3
X_future = pd.DataFrame({"t": np.arange(len(X) + future_periods)})
y_pred = model.predict(X_future)

# Add moving average for visual smoothing
df["moving_avg"] = df["monthly_total"].rolling(window=3).mean()

# Plot everything
plt.figure(figsize=(12, 6))
plt.plot(X["t"], y, label="Actual Sales", marker='o')
plt.plot(X["t"], df["moving_avg"], label="3-Month Moving Average", linestyle='--', color='green')
plt.plot(X_future["t"], y_pred, label="XGBoost Forecast (Next 3 Months)", linestyle='--', color='orange')
plt.title("Monthly Sales: Actual, Smoothed, and Forecasted")
plt.xlabel("Time Index")
plt.ylabel("Total Sales ($)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("forecast_xgb_plot.png")
plt.show()
