from flask import Flask, render_template, jsonify
import threading
import time
import random

app = Flask(__name__)

# Simulated route data with start/end points and labels for markers
route_data = [
]

# Background task to periodically update route data
def background_task():
    from angel import runit, GLOBAL_QUEUE
    runit()
    global route_data
    while True:
        newAngelRoutes = []
        ##Drain the queue - get the last published updates
        try:
            while True:
                newAngelRoutes = GLOBAL_QUEUE.get(block=False)
        except:
            pass
        if len(newAngelRoutes)>0:
            print('Got these new routes',newAngelRoutes)
            route_data.clear()
            for r in newAngelRoutes:
                new_route = {
                    "start": [float(r['origin']['lat']), float(r['origin']['lon'])],
                    "end": [float(r['destination']['lat']), float(r['destination']['lon'])],
                    "color": random.choice(["red", "blue", "green", "yellow"]),
                    "start_marker": f"Origin: {r['origin']['name']} {r['originStatus']['num_bikes_available']}|{r['originStatus']['num_docks_available']}",
                    "end_marker": f"Destination: {r['destination']['name']} {r['destinationStatus']['num_bikes_available']}|{r['destinationStatus']['num_docks_available']}",
                }
                route_data.append(new_route)
            route_data.append(new_route)

        time.sleep(10)

# Start background task to update route data
thread = threading.Thread(target=background_task)
thread.daemon = True
thread.start()

# Route to serve the map HTML (render map.html)
@app.route('/')
def index():
    return render_template('map.html')  # Ensure that map.html is in the templates directory

# Route to provide the current route data as JSON
@app.route('/routes', methods=['GET'])
def get_routes():
    return jsonify({"routes": route_data})

if __name__ == '__main__':
    app.run(debug=True)
