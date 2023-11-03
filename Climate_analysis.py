# Climate challenge SQL

#installing library sqlalchemy
pip install sqlalchemy


import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt
from flask import Flask, request, jsonify 


# Connecting to the SQLite database
engine = sqlalchemy.create_engine('sqlite:///hawaii.sqlite')

# Database schema
Base = sqlalchemy.ext.declarative.declarative_base()
Base.metadata.reflect(engine)

# Referencias das tabelas
Station = Base.classes.station
Measurement = Base.classes.measurement

# Cria a session
session = sqlalchemy.orm.Session(engine)


# Precipitation Analysis

# Find the most recent date in the dataset
recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]

# Get the previous 12 months of precipitation data
previous_months_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= recent_date - sqlalchemy.DateDelta(days=365)).all()

# Convert the query results to a Pandas DataFrame
df_precipitation = pd.DataFrame(previous_months_data, columns=['date', 'prcp'])

# Sort the DataFrame by date using method sort values
df_precipitation = df_precipitation.sort_values(by='date')

# Plot the results
plt.figure()
plt.plot(df_precipitation['date'], df_precipitation['prcp'])
plt.xlabel('Date')
plt.ylabel('Precipitation')
plt.title('Precipitation in Honolulu, HI')
plt.show()

print(df_precipitation.describe())

# Analysis

# Calculate the total number of stations in the dataset
number_of_stations = session.query(Station.id).count()

# List the stations and observation counts in descending order
station_counts = session.query(Station.id, Station.name, Station.latitude, Station.longitude, Station.elevation, sqlalchemy.func.count(Measurement.id).label('observation_count')).group_by(Station.id).order_by(sqlalchemy.func.count(Measurement.id).desc()).all()

# Find the most active station
most_active_station = station_counts[0][0]

# Calculate the lowest, highest, and average temperatures for the most active station
lowest_temperature = session.query(Measurement.tobs).filter(Measurement.station == most_active_station).order_by(Measurement.tobs.asc()).first()[0]
highest_temperature = session.query(Measurement.tobs).filter(Measurement.station == most_active_station).order_by(Measurement.tobs.desc()).first()[0]
average_temperature = session.query(sqlalchemy.func.avg(Measurement.tobs)).filter(Measurement.station == most_active_station).scalar()

# Get the previous 12 months of temperature observation data for the most active station
prev_12_months_tobs = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station, Measurement.date >= recent_date - sqlalchemy.DateDelta(days=365)).all()

# Convert the query results to a Pandas DataFrame
tobs_df = pd.DataFrame(prev_12_months_tobs, columns=['date', 'tobs'])

# Plot the results as a histogram 
plt.figure()
plt.hist(tobs_df['tobs'], bins=12)
plt.xlabel('Temperature (F)')
plt.ylabel('Frequency')
plt.title('Temperature Distribution in Honolulu, HI for the Most Active Station')
plt.show()

# Close the SQLAlchemy session
session.close()

# Flask API

# Create a Flask app
app = Flask(__name__)

# Landing page
@app.route('/', methods=['GET'])
def landing_page():
    available_routes = {
        '/precipitation': 'Returns json with the date as the key and the value as the precipitation for the last year in the database',
        '/stations': 'Returns jsonified data of all of the stations in the database',
        '/tobs': 'Returns jsonified data for the most active station (USC00519281) for the last year in the database',
        '/start/<start>': 'Accepts the start date as a parameter from the URL and returns the min, max, and average temperatures calculated from the given start date to the end of the dataset'
        