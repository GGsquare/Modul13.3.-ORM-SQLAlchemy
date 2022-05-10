import csv
import sqlite3

try:
    # Import csv and extract data
    with open('Modul13/Zadanie2(13.3. ORM SQLAlchemy)/clean_measure.csv', 'r') as fin:
        dr = csv.DictReader(fin)
        measure_info = [(i['station'], i['date'], i['precip'], i['tobs']) for i in dr]
        print(measure_info)


    with open('Modul13/Zadanie2(13.3. ORM SQLAlchemy)/clean_stations.csv', 'r') as fin:
        dr = csv.DictReader(fin)
        stations_info=[(i['station'], i['latitude'], i['longitude'], i['elevation'], i['name'], i['country'], i['state']) for i in dr]
        print(stations_info)

    # Connect to SQLite
    sqliteConnection = sqlite3.connect('sql.db')
    cursor = sqliteConnection.cursor()
  
    # Create tables
    cursor.execute('create table measure(station varchar2(10), date int, precip int, tobs int);')
  
    cursor.execute('create table stations(station varchar2(10), latitude int, longitude int, elevation int, name str, country str, state str);')

    # Insert data into tables
    cursor.executemany(
        "insert into measure (station, date, precip, tobs) VALUES (?, ?, ?, ?);", measure_info)
    
    cursor.executemany(
        "insert into stations (station, latitude, longitude, elevation, name, country, state) VALUES (?, ?, ?, ?, ?, ?, ?);", stations_info)
    
    # Show tables
    cursor.execute('select * from measure;')

    cursor.execute('select * from stations;')
  
    # View result
    result = cursor.fetchall()
    print(result)
  
    # Commit work and close connection
    sqliteConnection.commit()
    cursor.close()
  
except sqlite3.Error as error:
    print('Error occured - ', error)
  
finally:
    if sqliteConnection:
        sqliteConnection.close()
        print('SQLite Connection closed')