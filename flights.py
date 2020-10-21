import datetime
from datetime import datetime

import schedule
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["flightStats"]

mycol = mydb["flights"]

from pandas import DataFrame

from Flights.untitled.flightstats_api_client.api_clients import FlightStatusByRouteAPIClient

app_id = 'f321c194'
app_key = 'f9a45dc0278258dba033cb4805a05832'
client = FlightStatusByRouteAPIClient(app_id, app_key)

departure_airport_code = 'SEA'
arrival_airport_code = 'ORD'
departure_date = '2020-03-28'  # Must be in yyyy-mm-dd format

response = client.get_flights_by_departure_date(departure_airport_code,
                                                arrival_airport_code,
                                                departure_date)





def job():
 mycol.delete_many({})
 df = DataFrame(response, columns=['flightStatuses'])
 for hbe in range(0, len(df)):
   mydict = {"flightId": df['flightStatuses'][hbe]['flightId'], "carrierFsCode": df['flightStatuses'][hbe]['carrierFsCode'], "flightNumber": df['flightStatuses'][hbe]['flightNumber'], "departureAirportFsCode": df['flightStatuses'][hbe]['departureAirportFsCode'],
             "arrivalAirportFsCode": df['flightStatuses'][hbe]['arrivalAirportFsCode'], "status": df['flightStatuses'][hbe]['status'], "departureDate_dateUtc": df['flightStatuses'][hbe]['departureDate']['dateUtc'], "departureDate_dateLocal": df['flightStatuses'][hbe]['departureDate']['dateLocal'],
             "arrivalDate_dateUtc": df['flightStatuses'][hbe]['arrivalDate']['dateUtc'], "arrivalDate_dateLocal": df['flightStatuses'][hbe]['arrivalDate']['dateLocal'],
             "flightType": df['flightStatuses'][hbe]['schedule']['flightType'],
             "serviceClasses": df['flightStatuses'][hbe]['schedule']['serviceClasses'], "restrictions": df['flightStatuses'][hbe]['schedule']['restrictions'],
             "o_publishedDeparture_dateUtc": df['flightStatuses'][hbe]['operationalTimes']['publishedDeparture']['dateUtc'],
             "o_publishedDeparture_dateLocal": df['flightStatuses'][hbe]['operationalTimes']['publishedDeparture']['dateLocal'],
             "o_scheduledGateDeparture_dateUtc": df['flightStatuses'][hbe]['operationalTimes']['scheduledGateDeparture']['dateUtc'],
             "o_scheduledGateDeparture_dateLocal": df['flightStatuses'][hbe]['operationalTimes']['scheduledGateDeparture']['dateLocal'],
             "o_publishedArrival_dateUtc": df['flightStatuses'][hbe]['operationalTimes']['publishedArrival']['dateUtc'],
             "o_publishedArrival_dateLocal": df['flightStatuses'][hbe]['operationalTimes']['publishedArrival']['dateLocal'],
             "o_scheduledGateArrival_dateUtc": df['flightStatuses'][hbe]['operationalTimes']['scheduledGateArrival']['dateUtc'],
             "o_scheduledGateArrival_dateLocal": df['flightStatuses'][hbe]['operationalTimes']['scheduledGateArrival']['dateLocal'],
             "Update_date": datetime.today().strftime('%Y-%m-%d-%H:%M:%S')}

   x = mycol.insert_one(mydict)

schedule.every(2).minutes.do(job)
while True:
 schedule.run_pending()
time.sleep(1)
print()






