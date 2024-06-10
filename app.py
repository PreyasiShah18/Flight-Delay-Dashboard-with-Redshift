from flask import Flask, render_template, jsonify
import psycopg2

app = Flask(__name__)

# Database connection
def get_db_connection():
    conn = psycopg2.connect(
        dbname='dev',
        user='admin',
        password='Flightdashboard2',
        host='my-flight-redshift-cluster.ctbvnucicxel.us-east-2.redshift.amazonaws.com',
        port='5439'
    )
    return conn

@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT airline, AVG(departure_delay), AVG(arrival_delay) FROM flight_delays GROUP BY airline')
    delays = cur.fetchall()
    cur.close()
    conn.close()
    print("Delays fetched from database:", delays)  # Debug statement to check retrieved data
    return render_template('index.html', delays=delays)

if __name__ == '__main__':
    app.run(debug=True)
