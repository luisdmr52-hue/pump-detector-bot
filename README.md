# 🚀 Pump Detector Bot

This bot watches Binance **spot markets** and alerts you on **Telegram** when it sees unusual price + volume pumps.  
It runs on a **VPS** (like Contabo) 24/7.

---

## 🔧 Features
- Monitors **multiple symbols** at the same time.  
- Checks **3 timeframes in parallel** (15m, 30m, 1h).  
- Uses **config.yaml** for all thresholds (easy to edit).  
- Sends **Telegram alerts** with price, % move, volume spike, and more.  
- Built-in **cooldowns** so you don’t get spammed.  
- **Hot-reloads config.yaml** automatically every 5 seconds (no restart needed).  

---

## 📦 Requirements
- Python 3.9+  
- Install dependencies:
  ```bash
  pip install websockets requests python-dotenv pyyaml uvloop
