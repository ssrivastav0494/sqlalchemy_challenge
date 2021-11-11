# Import Dependencies
from flask import Flask, jsonify
import numpy as np
import datetime as dt
from datetime import datetime

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.pool import StaticPool

# Database Setup
# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources\hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Flask Setup
app = Flask(__name__)

# Function to query recent date in the Measurement dataset
def recent_date():
    # Create our session (link) from Python to the DB
        session = Session(engine)
        
        # Query to get recent date
        recent_date_query = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
        recent_date = str(recent_date_query)

        session.close()

        return(recent_date[2:-3])

# Function to query first date in the Measurement dataset
def first_date():
    # Create our session (link) from Python to the DB
        session = Session(engine)
        
        # Query to get recent date
        first_date_query = session.query(Measurement.date).order_by(Measurement.date).first()
        first_date = str(first_date_query)

        session.close()

        return(first_date[2:-3])


# Define homepage route and list all the available routes
@app.route("/")
def home():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

# Define precipitation route
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # Query to retrieve previous year's date from the recent date function
    previous_year_date = datetime.strptime(recent_date(), '%Y-%m-%d') - dt.timedelta(days=365)
    
    # Query Date and Precipitation values for that one year
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= previous_year_date.date()).\
        group_by(Measurement.date).order_by(Measurement.date).all()
    
    session.close()

    # Create a dictionary from row data where date is key and precipitation is value
    precipitation = []
    for date, prcp in results:
        precipitation_dict = {}
        precipitation_dict[date] = prcp
        precipitation.append(precipitation_dict)

    return jsonify(precipitation)

# Define station route
@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query Station Names
    results = session.query(Station.name).all()

    session.close()

    station_names = list(np.ravel(results))
    return jsonify(station_names)

# Define temperature route
@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query for most active station
    most_active_station = session.query(Measurement.station).\
        group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
    
    # Query to retrieve previous year's date from the recent date function
    previous_year_date = datetime.strptime(recent_date(), '%Y-%m-%d') - dt.timedelta(days=365)

    results = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= previous_year_date.date()).\
            filter(Measurement.station == most_active_station[0]).all()
    
    session.close()

    # Create a dictionary from row data
    temperature_obs = []
    for date, tobs in results:
        temperature_obs_dict = {}
        temperature_obs_dict["Date"] = date
        temperature_obs_dict["Temperature"] = tobs
        temperature_obs.append(temperature_obs_dict)

    return jsonify(temperature_obs)

# Define start date route
@app.route("/api/v1.0/<start>")
def temp_start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # If condition to determine if start date entered is in the range
    if (first_date() <= start <= recent_date()) == True:
        
        # Parse start date into datetime
        start_date = datetime.strptime(start, '%Y-%m-%d')

        # Query TMAX, TMIN and TAVG for all dates after start date (inclusive)
        results = session.query(Measurement.date, func.max(Measurement.tobs), func.min(Measurement.tobs), func.avg(Measurement.tobs)).\
            filter(Measurement.date >= start_date.date()).group_by(Measurement.date).all()
        
        session.close()

        tmax = results[0][1]
        tmin = results[0][2]
        tavg = results[0][3]

        # Create a dictionary from row data
        temp_start = []
        for date, tmax, tmin, tavg in results:
            temp_start_dict = {}
            temp_start_dict["Date"] = date
            temp_start_dict["TMAX"] = round(tmax,2)
            temp_start_dict["TMIN"] = round(tmin,2)
            temp_start_dict["TAVG"] = round(tavg,2)
            temp_start.append(temp_start_dict)

        return jsonify(temp_start)
    
    return f"Please enter start date between {first_date()} and {recent_date()} (Format: YYYY-MM-DD)"

# Define start and end date route
@app.route("/api/v1.0/<start>/<end>")
def temp_start_end(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    # If condition to determine if start and end date entered is in the range
    if (((first_date() <= start <= recent_date()) == True) & ((first_date() <= end <= recent_date()) == True)) == True:
        
        # Parse start date into datetime
        start_date = datetime.strptime(start,'%Y-%m-%d')
        end_date = datetime.strptime(end,'%Y-%m-%d')

        # Query TMAX, TMIN and TAVG for all dates between start and end date (inclusive)
        results = session.query(Measurement.date, func.max(Measurement.tobs), func.min(Measurement.tobs), func.avg(Measurement.tobs)).\
            filter(Measurement.date >= start_date.date(), Measurement.date <= end_date.date()).\
                group_by(Measurement.date).all()
        
        session.close()

        tmax = results[0][1]
        tmin = results[0][2]
        tavg = results[0][3]

        # Create a dictionary from row data
        temp_start_end = []
        for date, tmax, tmin, tavg in results:
            temp_start_end_dict = {}
            temp_start_end_dict["Date"] = date
            temp_start_end_dict["TMAX"] = round(tmax,2)
            temp_start_end_dict["TMIN"] = round(tmin,2)
            temp_start_end_dict["TAVG"] = round(tavg,2)
            temp_start_end.append(temp_start_end_dict)

        return jsonify(temp_start_end)
    
    return f"Please enter start & end date between {first_date()} and {recent_date()} (Format: YYYY-MM-DD)"

if __name__ == "__main__":
    app.run(debug=True)