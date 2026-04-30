# [Moch. Rizki Nasrullah - 5027241038]

from flask import Flask, render_template, jsonify
from datetime import datetime
import json
import os

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

LIVE_API_FILE     = os.path.join(DATA_DIR, "live_api.json")
LIVE_RSS_FILE     = os.path.join(DATA_DIR, "live_rss.json")
SPARK_RESULT_FILE = os.path.join(DATA_DIR, "spark_results.json")


def read_json_file(filepath, default):
    if not os.path.exists(filepath):
        return default
    try:
        with open(filepath, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"[app.py] Gagal baca {filepath}: {e}")
        return default


def get_aqi_color(aqi):
    if aqi is None: return "#808080"
    aqi = float(aqi)
    if aqi <= 50:  return "#00e400"
    if aqi <= 100: return "#ffff00"
    if aqi <= 150: return "#ff7e00"
    if aqi <= 200: return "#ff0000"
    return "#7e0023"


def get_aqi_label(aqi):
    if aqi is None: return "N/A"
    aqi = float(aqi)
    if aqi <= 50:  return "Baik"
    if aqi <= 100: return "Sedang"
    if aqi <= 150: return "Tidak Sehat"
    if aqi <= 200: return "Sangat Tidak Sehat"
    return "Berbahaya"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/summary")
def api_summary():
    results = read_json_file(SPARK_RESULT_FILE, None)
    if results is None:
        return jsonify({
            "status": "no_data",
            "message": "Jalankan spark/analysis.ipynb terlebih dahulu.",
            "total_records": 0, "kota_terburuk": "-",
            "avg_aqi": 0, "pct_tidak_sehat": 0, "generated_at": "-"
        })
    s = results.get("summary", {})
    return jsonify({
        "status": "ok",
        "total_records":   s.get("total_records", 0),
        "kota_terburuk":   s.get("kota_terburuk", "-"),
        "avg_aqi":         s.get("avg_aqi_all_cities", 0),
        "pct_tidak_sehat": s.get("pct_tidak_sehat", 0),
        "generated_at":    results.get("generated_at", "-")
    })


@app.route("/api/ranking")
def api_ranking():
    results = read_json_file(SPARK_RESULT_FILE, None)
    if results is None:
        return jsonify({"status": "no_data", "data": []})
    ranking = results.get("analisis_3_ranking", [])
    for row in ranking:
        row["color"] = get_aqi_color(row.get("rata_rata_aqi"))
        row["label"] = get_aqi_label(row.get("rata_rata_aqi"))
    return jsonify({"status": "ok", "data": ranking})


@app.route("/api/per-jam")
def api_per_jam():
    results = read_json_file(SPARK_RESULT_FILE, None)
    if results is None:
        return jsonify({"status": "no_data", "data": []})
    return jsonify({"status": "ok", "data": results.get("analisis_2_per_jam", [])})


@app.route("/api/distribusi")
def api_distribusi():
    results = read_json_file(SPARK_RESULT_FILE, None)
    if results is None:
        return jsonify({"status": "no_data", "data": []})
    return jsonify({"status": "ok", "data": results.get("analisis_1_distribusi", [])})


@app.route("/api/live-aqi")
def api_live_aqi():
    # Baca live_api.json yang ditulis consumer_to_hdfs.py
    events = read_json_file(LIVE_API_FILE, [])
    events = list(reversed(events[-20:]))
    for e in events:
        e["color"] = get_aqi_color(e.get("aqi"))
        e["label"] = get_aqi_label(e.get("aqi"))
    return jsonify({"status": "ok", "count": len(events), "data": events})


@app.route("/api/live-rss")
def api_live_rss():
    # Baca live_rss.json yang ditulis consumer_to_hdfs.py
    articles = read_json_file(LIVE_RSS_FILE, [])
    articles = list(reversed(articles[-10:]))
    return jsonify({"status": "ok", "count": len(articles), "data": articles})


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "live_api_exists":     os.path.exists(LIVE_API_FILE),
        "live_rss_exists":     os.path.exists(LIVE_RSS_FILE),
        "spark_result_exists": os.path.exists(SPARK_RESULT_FILE)
    })


if __name__ == "__main__":
    print("=" * 55)
    print("  AirQuality Alert Dashboard — Kelompok 5 ETS")
    print("  Akses di: http://localhost:5000")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5000, debug=True)
