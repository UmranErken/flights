"""Defines FlightStats.com By Route API clients module"""
import requests

from datetime import datetime

DEFAULT_DATE_FORMAT = '%Y-%m-%d'
URL_TEMPLATE = 'https://api.flightstats.com/flex/flightstatus/rest/v2/json/route/status/{departure_airport_code}/{arrival_airport_code}/dep/{year}/{month}/{day}?appId={appId}&appKey={appKey}&hourOfDay=0&utc=false&numHours=24&maxFlights={maxFlights}'


class FlightStatusByRouteAPIClient(object):


    def __init__(self, app_id: str, app_key: str) -> None:
        """Defines constructor

        Args:
            self: instance of FlightStatusByRouteAPIClient class
            app_id: a string represents flightstats.com apiId
            app_key: a string representsa flightstats.com apiKey
        """
        self.app_id = app_id
        self.app_key = app_key

    def get_flights_by_departure_date(self,
                                      departure_airport_code: str, 
                                      arrival_airport_code: str,
                                      departure_date: str,
                                      limit: int=500) -> list:
        """Gets flights from flightstats.com

        Args:
            departure_airport_code: A string represents departure airport code. 
            arrival_airport_code: A string represents arrival airport code.
            departure_date: A string represents departure date in yyyy-mm-dd 
                format
            limit: A integer limits number of flights returned
        Returns:
            A list of dictionary represents flights
        """
        date = datetime.strptime(departure_date, DEFAULT_DATE_FORMAT)
        url = URL_TEMPLATE.format(
            appId=self.app_id,
            appKey=self.app_key,
            departure_airport_code=departure_airport_code,
            arrival_airport_code=arrival_airport_code,
            year=date.year,
            month=date.month,
            day=date.day,
            maxFlights=limit
        )
        response = requests.get(url)
        return response.json()




