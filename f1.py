import time
from pprint import pprint, pformat
from utils.requests_wrapper import RequestsWrapper
from utils.azure_wrapper import AzureDBWrapper
from datetime import datetime
from utils.utils import get_years_between
from decimal import Decimal

# Base URL of the Ergast API
base_url = 'https://ergast.com/api/f1'


class Season(object):
    def __init__(self):
        self.year = None
        self.url = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class Driver(object):
    def __init__(self):
        self.driver_id = None
        self.number = None
        self.code = None
        self.forename = None
        self.surname = None
        self.date_of_birth = None
        self.nationality = None
        self.url = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class Constructor(object):
    def __init__(self):
        self.constructor_id = None
        self.name = None
        self.nationality = None
        self.url = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class Status(object):
    def __init__(self):
        self.id = None
        self.status = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class Circuit(object):
    def __init__(self):
        self.circuit_id = None
        self.name = None
        self.location = None
        self.country = None
        self.latitude = None
        self.longitude = None
        self.altitude = None
        self.url = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class Race(object):
    def __init__(self):
        self.year = None
        self.round = None
        self.circuit_id = None
        self.name = None
        self.date = None
        self.time = None
        self.url = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class DriverStandings(object):
    def __init__(self):
        self.driver_id = None
        self.points = None
        self.position = None
        self.wins = None
        self.year = None
        self.round = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class ConstructorStandings(object):
    def __init__(self):
        self.constructor_id = None
        self.points = None
        self.position = None
        self.wins = None
        self.year = None
        self.round = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class Qualifying(object):
    def __init__(self):
        self.driver_id = None
        self.constructor_id = None
        self.year = None
        self.round = None
        self.number = None
        self.position = None
        self.q1 = None
        self.q2 = None
        self.q3 = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class PitStop(object):
    def __init__(self):
        self.driver_id = None
        self.stop = None
        self.lap = None
        self.time = None
        self.duration = None
        self.milliseconds = None
        self.year = None
        self.round = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class LapTime(object):
    def __init__(self):
        self.driver_id = None
        self.lap = None
        self.position = None
        self.time = None
        self.milliseconds = None
        self.year = None
        self.round = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class Result(object):
    def __init__(self):
        self.driver_id = None
        self.constructor_id = None
        self.number = None
        self.grid = None
        self.position = None
        self.points = None
        self.laps = None
        self.milliseconds = None
        self.fastest_lap = None
        self.rank = None
        self.fastest_lap_time = None
        self.fastest_lap_speed = None
        self.status_id = None
        self.year = None
        self.round = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class SprintResult(object):
    def __init__(self):
        self.driver_id = None
        self.constructor_id = None
        self.number = None
        self.grid = None
        self.position = None
        self.points = None
        self.laps = None
        self.milliseconds = None
        self.fastest_lap = None
        self.fastest_lap_time = None
        self.status_id = None
        self.year = None
        self.round = None

    def __repr__(self):
        return pformat(vars(self), indent=2, width=1)


class F1DataCollector(object):
    def __init__(self):
        self.entity = self.__class__.__base__.__name__
        self.request = RequestsWrapper()
        self.azure_db = AzureDBWrapper()

        self.env = dict()

    def __repr__(self):
        return f'{self.__class__.__name__}'


class SeasonCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'seasons.json'
        self.seasons_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_seasons_data()
        if self.seasons_data:
            response = self._store_seasons_data()
            print(f'Seasons rows: {response}')
        else:
            print(f'No new seasons data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['start_year'] = None
        self.env['end_year'] = datetime.now().year

        # Collect seasons information
        seasons_query = """
            SELECT MAX(seasons.year) + 1 AS start_year
            FROM seasons;
        """
        seasons_raw = self.azure_db.select(query=seasons_query)

        for row in seasons_raw:
            self.env['start_year'] = row['start_year']

    def _collect_seasons_data(self):
        if self.env['start_year'] > self.env['end_year']:
            return

        years = get_years_between(self.env['start_year'], self.env['end_year'])

        response = self.request.get(target=self.target)
        if response.status == 200:
            # offset = response.offset
            data = response.data
            mr_data = data['MRData']
            season_table = mr_data['SeasonTable']
            seasons = season_table['Seasons']
            for season in seasons:
                if int(season['season']) in years:
                    season_row = Season()
                    season_row.year = season['season']
                    season_row.url = season['url']
                    self.seasons_data.append(season_row)

    def _store_seasons_data(self):
        """
            Store seasons data
        :return: The query response
        """
        return self.azure_db.insert(table_name='seasons', data=self.seasons_data)


class DriverCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = '2024/drivers.json'
        self.drivers_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_drivers_data()
        if self.drivers_data:
            response = self._store_drivers_data()
            print(f'Driver rows: {response}')
        else:
            print(f'No new drivers data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['drivers_in_db'] = list()

        # Collect drivers information
        drivers_query = """
            SELECT drivers.driver_id
            FROM drivers;
        """
        drivers_raw = self.azure_db.select(query=drivers_query)

        for row in drivers_raw:
            self.env['drivers_in_db'].append(row['driver_id'])

    def _collect_drivers_data(self):

        response = self.request.get(target=self.target)
        if response.status == 200:
            # offset = response.offset
            data = response.data
            mr_data = data['MRData']
            driver_table = mr_data['DriverTable']
            drivers = driver_table['Drivers']
            for driver in drivers:
                if driver['driverId'] not in self.env['drivers_in_db']:
                    driver_row = Driver()
                    driver_row.driver_id = driver['driverId']
                    driver_row.number = driver['permanentNumber']
                    driver_row.code = driver['code']
                    driver_row.forename = driver['givenName']
                    driver_row.surname = driver['familyName']
                    driver_row.date_of_birth = driver['dateOfBirth']
                    driver_row.nationality = driver['nationality']
                    driver_row.url = driver['url']

                    self.drivers_data.append(driver_row)

    def _store_drivers_data(self):
        """
            Store drivers data
        :return: The query response
        """
        return self.azure_db.insert(table_name='drivers', data=self.drivers_data)


class ConstructorCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = '2024/constructors.json'
        self.constructors_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_constructors_data()
        if self.constructors_data:
            response = self._store_constructors_data()
            print(f'Constructor rows: {response}')
        else:
            print(f'No new constructors data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['constructors_in_db'] = list()

        # Collect constructors information
        constructors_query = """
            SELECT constructors.constructor_id
            FROM constructors;
        """
        constructors_raw = self.azure_db.select(query=constructors_query)

        for row in constructors_raw:
            self.env['constructors_in_db'].append(row['constructor_id'])

    def _collect_constructors_data(self):

        response = self.request.get(target=self.target)
        if response.status == 200:
            # offset = response.offset
            data = response.data
            mr_data = data['MRData']
            constructor_table = mr_data['ConstructorTable']
            constructors = constructor_table['Constructors']
            for constructor in constructors:
                if constructor['constructorId'] not in self.env['constructors_in_db']:
                    constructor_row = Constructor()
                    constructor_row.constructor_id = constructor['constructorId']
                    constructor_row.name = constructor['name']
                    constructor_row.nationality = constructor['nationality']
                    constructor_row.url = constructor['url']

                    self.constructors_data.append(constructor_row)

    def _store_constructors_data(self):
        """
            Store constructors data
        :return: The query response
        """
        return self.azure_db.insert(table_name='constructors', data=self.constructors_data)


class StatusCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'status.json'
        self.status_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_status_data()
        if self.status_data:
            response = self._store_status_data()
            print(f'Status rows: {response}')
        else:
            print(f'No new status data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['statuses_in_db'] = list()

        # Collect status information
        status_query = """
            SELECT status.id
            FROM status;
        """
        status_raw = self.azure_db.select(query=status_query)

        for row in status_raw:
            self.env['statuses_in_db'].append(row['id'])

    def _collect_status_data(self):

        response = self.request.get(target=self.target)
        if response.status == 200:
            # offset = response.offset
            data = response.data
            mr_data = data['MRData']
            status_table = mr_data['StatusTable']
            statuses = status_table['Status']
            for status in statuses:
                if int(status['statusId']) not in self.env['statuses_in_db']:
                    status_row = Constructor()
                    status_row.id = status['statusId']
                    status_row.status = status['status']

                    self.status_data.append(status_row)

    def _store_status_data(self):
        """
            Store constructors data
        :return: The query response
        """
        return self.azure_db.insert(table_name='status', data=self.status_data)


class CircuitCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = '2023/circuits.json'
        self.circuits_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_circuits_data()
        if self.circuits_data:
            response = self._store_circuits_data()
            print(f'Circuits rows: {response}')
        else:
            print(f'No new circuits data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['circuits_in_db'] = list()

        # Collect circuits information
        circuits_query = """
            SELECT circuits.circuit_id
            FROM circuits;
        """
        circuits_raw = self.azure_db.select(query=circuits_query)

        for row in circuits_raw:
            self.env['circuits_in_db'].append(row['circuit_id'])

    def _collect_circuits_data(self):

        response = self.request.get(target=self.target)
        if response.status == 200:
            # offset = response.offset
            data = response.data
            mr_data = data['MRData']
            circuits_table = mr_data['CircuitTable']
            circuits = circuits_table['Circuits']
            for circuit in circuits:
                if circuit['circuitId'] not in self.env['circuits_in_db']:
                    circuit_row = Circuit()
                    circuit_row.circuit_id = circuit['circuitId']
                    circuit_row.name = circuit['circuitName']
                    circuit_row.location = circuit['Location']['locality']
                    circuit_row.country = circuit['Location']['country']
                    circuit_row.latitude = circuit['Location']['lat']
                    circuit_row.longitude = circuit['Location']['long']
                    circuit_row.url = circuit['url']

                    self.circuits_data.append(circuit_row)

    def _store_circuits_data(self):
        """
            Store circuits data
        :return: The query response
        """
        return self.azure_db.insert(table_name='status', data=self.circuits_data)


class RaceCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'races.json'
        self.races_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_races_data()
        if self.races_data:
            response = self._store_races_data()
            print(f'Races rows: {response}')
        else:
            print(f'No new races data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['races_in_db'] = datetime.min

        # Collect circuits information
        races_query = """
            SELECT MAX(races.date) AS max_date
            FROM races;
        """
        races_raw = self.azure_db.select(query=races_query)

        for row in races_raw:
            self.env['max_race'] = row['max_date']

    def _collect_races_data(self):

        offset = 0
        params = None

        while True:
            if offset:
                params = dict()
                params['offset'] = 1000
            response = self.request.get(target=self.target, parameters=params)
            if response.status == 200:
                data = response.data
                mr_data = data['MRData']

                races_table = mr_data['RaceTable']
                races = races_table['Races']
                for race in races:
                    race_date = datetime.strptime(race['date'], '%Y-%m-%d').date()
                    if race_date > self.env['max_race']:
                        race_row = Race()
                        race_row.year = race['season']
                        race_row.round = race['round']
                        race_row.circuit_id = race['Circuit']['circuitId']
                        race_row.name = race['raceName']
                        race_row.date = race_date
                        race_row.url = race['url']

                        self.races_data.append(race_row)

                total_rows = int(mr_data.get('total', 0))
                batch_size = int(mr_data.get('limit', 0))
                offset = offset + batch_size
                if offset > total_rows:
                    break

    def _store_races_data(self):
        """
            Store circuits data
        :return: The query response
        """
        return self.azure_db.insert(table_name='races', data=self.races_data)


class DriverStandingsCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'driverStandings.json'
        self.driver_standings_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_driver_standings_data()
        if self.driver_standings_data:
            response = self._store_driver_standings_data()
            print(f'Driver standings rows: {response}')
        else:
            print(f'No new driver standings data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['driver_standings_data_in_db'] = dict()

        # Collect circuits information
        driver_standings_query = """
            SELECT max_year.max_year,
                   MAX(driver_standings.round) AS max_round
            FROM driver_standings
                     INNER JOIN (SELECT MAX(driver_standings.year) AS max_year
                                 FROM driver_standings) AS max_year ON max_year.max_year = driver_standings.year
            WHERE driver_standings.year = max_year.max_year
            GROUP BY max_year.max_year;
        """
        driver_standings_raw = self.azure_db.select(query=driver_standings_query)

        for row in driver_standings_raw:
            self.env['driver_standings_data_in_db'] = {'year': int(row['max_year']),
                                                       'round': int(row['max_round'])}

    def _collect_driver_standings_data(self):

        if self.env['driver_standings_data_in_db']:
            max_year = self.env['driver_standings_data_in_db']['year']
            max_round = self.env['driver_standings_data_in_db']['round'] + 1
            for year in range(max_year, datetime.now().year + 1):
                current_round = max_round if max_round else 1

                while current_round:
                    request_target = f'{year}/{current_round}/{self.target}'
                    offset = 0
                    params = None

                    while True:
                        if offset:
                            params = dict()
                            params['offset'] = 1000
                        response = self.request.get(target=request_target, parameters=params)

                        if response.status == 200:
                            data = response.data
                            mr_data = data['MRData']
                            standings_table = mr_data['StandingsTable']
                            standings = standings_table['StandingsLists']

                            if standings:
                                current_round += 1
                            else:
                                current_round = None
                                max_round = None

                            for standing in standings:
                                for driver in standing['DriverStandings']:
                                    standing_row = DriverStandings()
                                    standing_row.driver_id = driver['Driver']['driverId']
                                    standing_row.points = driver['points']
                                    standing_row.position = driver['position']
                                    standing_row.wins = driver['wins']
                                    standing_row.year = standing['season']
                                    standing_row.round = standing['round']

                                    self.driver_standings_data.append(standing_row)

                            total_rows = int(mr_data.get('total', 0))
                            batch_size = int(mr_data.get('limit', 0))
                            offset = offset + batch_size
                            if offset > total_rows:
                                break

    def _store_driver_standings_data(self):
        """
            Store driver standings data
        :return: The query response
        """
        return self.azure_db.insert(table_name='driver_standings', data=self.driver_standings_data)


class ConstructorStandingsCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'constructorStandings.json'
        self.constructor_standings_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_constructor_standings_data()
        if self.constructor_standings_data:
            response = self._store_constructor_standings_data()
            print(f'Constructor standings rows: {response}')
        else:
            print(f'No new constructor standings data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['constructor_standings_data_in_db'] = dict()

        # Collect circuits information
        constructor_standings_query = """
            SELECT max_year.max_year,
                   MAX(constructor_standings.round) AS max_round
            FROM constructor_standings
                     INNER JOIN (SELECT MAX(constructor_standings.year) AS max_year
                                 FROM constructor_standings) AS max_year ON max_year.max_year = constructor_standings.year
            WHERE constructor_standings.year = max_year.max_year
            GROUP BY max_year.max_year;
        """
        constructor_standings_raw = self.azure_db.select(query=constructor_standings_query)

        for row in constructor_standings_raw:
            self.env['constructor_standings_data_in_db'] = {'year': int(row['max_year']),
                                                            'round': int(row['max_round'])}

    def _collect_constructor_standings_data(self):
        if self.env['constructor_standings_data_in_db']:
            max_year = self.env['constructor_standings_data_in_db']['year']
            max_round = self.env['constructor_standings_data_in_db']['round'] + 1
            for year in range(max_year, datetime.now().year + 1):
                current_round = max_round if max_round else 1

                while current_round:
                    request_target = f'{year}/{current_round}/{self.target}'
                    offset = 0
                    params = None

                    while True:
                        if offset:
                            params = dict()
                            params['offset'] = 1000
                        response = self.request.get(target=request_target, parameters=params)

                        if response.status == 200:
                            data = response.data
                            mr_data = data['MRData']
                            standings_table = mr_data['StandingsTable']
                            standings = standings_table['StandingsLists']

                            if standings:
                                current_round += 1
                            else:
                                current_round = None
                                max_round = None

                            for standing in standings:
                                for constructor in standing['ConstructorStandings']:
                                    standing_row = ConstructorStandings()
                                    standing_row.constructor_id = constructor['Constructor']['constructorId']
                                    standing_row.points = constructor['points']
                                    standing_row.position = constructor['position']
                                    standing_row.wins = constructor['wins']
                                    standing_row.year = standing['season']
                                    standing_row.round = standing['round']

                                    self.constructor_standings_data.append(standing_row)

                            total_rows = int(mr_data.get('total', 0))
                            batch_size = int(mr_data.get('limit', 0))
                            offset = offset + batch_size
                            if offset > total_rows:
                                break

    def _store_constructor_standings_data(self):
        """
            Store driver standings data
        :return: The query response
        """
        return self.azure_db.insert(table_name='constructor_standings', data=self.constructor_standings_data)


class QualifyingCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'qualifying.json'
        self.qualifying_data = list()

    def run(self, **kwargs):
        self._populate_env()
        self._collect_qualifying_data()
        if self.qualifying_data:
            response = self._store_qualifying_data()
            print(f'Qualifying rows: {response}')
        else:
            print(f'No new qualifying data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['max_qualifying_date'] = dict()

        # Collect qualifying information
        qualifying_query = """
            SELECT max_year.max_year,
                   MAX(qualifying.round) AS max_round
            FROM qualifying
                     INNER JOIN (SELECT MAX(qualifying.year) AS max_year
                                 FROM qualifying) AS max_year ON max_year.max_year = qualifying.year
            WHERE qualifying.year = max_year.max_year
            GROUP BY max_year.max_year;
        """
        qualifying_raw = self.azure_db.select(query=qualifying_query)

        for row in qualifying_raw:
            self.env['max_qualifying_date'] = {'year': int(row['max_year']),
                                               'round': int(row['max_round'])}

    def _collect_qualifying_data(self):
        if self.env['max_qualifying_date']:
            max_year = self.env['max_qualifying_date']['year']
            max_round = self.env['max_qualifying_date']['round'] + 1
            for year in range(max_year, datetime.now().year + 1):
                current_round = max_round if max_round else 1

                while current_round:
                    request_target = f'{year}/{current_round}/{self.target}'
                    offset = 0
                    params = None

                    while True:
                        if offset:
                            params = dict()
                            params['offset'] = 1000
                        response = self.request.get(target=request_target, parameters=params)

                        if response.status == 200:
                            data = response.data
                            mr_data = data['MRData']
                            race_table = mr_data['RaceTable']
                            races = race_table['Races']

                            if races:
                                current_round += 1
                            else:
                                current_round = None
                                max_round = None
                            for race in races:
                                for qualifying in race['QualifyingResults']:
                                    qualifying_row = Qualifying()
                                    qualifying_row.constructor_id = qualifying['Constructor']['constructorId']
                                    qualifying_row.driver_id = qualifying['Driver']['driverId']
                                    qualifying_row.number = qualifying['number']
                                    qualifying_row.position = qualifying['position']
                                    if qualifying.get('Q1') and qualifying.get('Q1') != '':
                                        q1 = datetime.strptime(qualifying['Q1'], '%M:%S.%f').time()
                                        qualifying_row.q1 = (q1.minute * 60000) + (q1.second * 1000) + (
                                                q1.microsecond / 1000)
                                    if qualifying.get('Q2') and qualifying.get('Q2') != '':
                                        q2 = datetime.strptime(qualifying['Q2'], '%M:%S.%f').time()
                                        qualifying_row.q2 = (q2.minute * 60000) + (q2.second * 1000) + (
                                                q2.microsecond / 1000)
                                    if qualifying.get('Q3') and qualifying.get('Q3') != '':
                                        q3 = datetime.strptime(qualifying['Q3'], '%M:%S.%f').time()
                                        qualifying_row.q3 = (q3.minute * 60000) + (q3.second * 1000) + (
                                                q3.microsecond / 1000)
                                    qualifying_row.year = race['season']
                                    qualifying_row.round = race['round']

                                    self.qualifying_data.append(qualifying_row)

                            total_rows = int(mr_data.get('total', 0))
                            batch_size = int(mr_data.get('limit', 0))
                            offset = offset + batch_size
                            if offset > total_rows:
                                break

    def _store_qualifying_data(self):
        """
            Store driver standings data
        :return: The query response
        """
        return self.azure_db.insert(table_name='qualifying', data=self.qualifying_data)


class PitStopsCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'pitstops.json'
        self.pit_stops_data = list()

    def run(self):
        self._populate_env()
        self._collect_pit_stops_data()
        if self.pit_stops_data:
            response = self._store_pit_stops_data()
            print(f'Pit stops rows: {response}')
        else:
            print(f'No new pit stops data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['max_pit_stops_date'] = dict()

        # Collect pit stops information
        pit_stops_query = """
            SELECT max_year.max_year,
                   MAX(pit_stops.round) AS max_round
            FROM pit_stops
                     INNER JOIN (SELECT MAX(pit_stops.year) AS max_year
                                 FROM pit_stops) AS max_year ON max_year.max_year = pit_stops.year
            WHERE pit_stops.year = max_year.max_year
            GROUP BY max_year.max_year;
        """
        pit_stops_raw = self.azure_db.select(query=pit_stops_query)

        for row in pit_stops_raw:
            self.env['max_pit_stops_date'] = {'year': int(row['max_year']),
                                              'round': int(row['max_round'])}

    def _collect_pit_stops_data(self, target=None):
        if self.env['max_pit_stops_date']:
            max_year = self.env['max_pit_stops_date']['year']
            max_round = self.env['max_pit_stops_date']['round'] + 1
            for year in range(max_year, datetime.now().year + 1):
                current_round = max_round if max_round else 1

                while current_round:
                    request_target = f'{year}/{current_round}/{self.target}'
                    offset = 0
                    params = None

                    while True:
                        if offset:
                            params = dict()
                            params['offset'] = 1000
                        response = self.request.get(target=request_target, parameters=params)

                        if response.status == 200:
                            data = response.data
                            mr_data = data['MRData']
                            race_table = mr_data['RaceTable']
                            races = race_table['Races']

                            if races:
                                current_round += 1
                            else:
                                current_round = None
                                max_round = None
                            for race in races:
                                for pit_stop in race['PitStops']:
                                    pit_stop_row = PitStop()
                                    pit_stop_row.driver_id = pit_stop['driverId']
                                    pit_stop_row.duration = pit_stop['duration']
                                    pit_stop_row.lap = pit_stop['lap']
                                    pit_stop_row.stop = pit_stop['stop']
                                    pit_stop_row.time = pit_stop['time']
                                    try:
                                        pit_stop_row.milliseconds = Decimal(pit_stop['duration']) * 1000
                                    except Exception as ex:
                                        pit_stop_time = datetime.strptime(pit_stop['duration'], '%M:%S.%f').time()
                                        pit_stop_row.milliseconds = (pit_stop_time.minute * 60000) + (
                                                pit_stop_time.second * 1000) + (pit_stop_time.microsecond / 1000)
                                    pit_stop_row.year = race['season']
                                    pit_stop_row.round = race['round']

                                    self.pit_stops_data.append(pit_stop_row)

                            total_rows = int(mr_data.get('total', 0))
                            batch_size = int(mr_data.get('limit', 0))
                            offset = offset + batch_size
                            if offset > total_rows:
                                break

    def _store_pit_stops_data(self):
        """
            Store pit stops data
        :return: The query response
        """
        return self.azure_db.insert(table_name='pit_stops', data=self.pit_stops_data)


class LapTimesCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'laps.json'
        self.laps_data = list()

    def run(self):
        self._populate_env()
        self._collect_laps_data()
        if self.laps_data:
            response = self._store_laps_data()
            print(f'Laps rows: {response}')
        else:
            print(f'No new laps data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['max_lap_times_date'] = dict()

        # Collect circuits information
        lap_times_query = """
            SELECT max_year.max_year,
                   MAX(lap_times.round) AS max_round
            FROM lap_times
                     INNER JOIN (SELECT MAX(lap_times.year) AS max_year
                                 FROM lap_times) AS max_year ON max_year.max_year = lap_times.year
            WHERE lap_times.year = max_year.max_year
            GROUP BY max_year.max_year;
        """
        lap_times_raw = self.azure_db.select(query=lap_times_query)

        for row in lap_times_raw:
            self.env['max_lap_times_date'] = {'year': int(row['max_year']),
                                              'round': int(row['max_round'])}

    def _collect_laps_data(self):
        if self.env['max_lap_times_date']:
            max_year = self.env['max_lap_times_date']['year']
            max_round = self.env['max_lap_times_date']['round'] + 1
            for year in range(max_year, datetime.now().year + 1):
                current_round = max_round if max_round else 1

                while current_round:
                    request_target = f'{year}/{current_round}/{self.target}'
                    offset = 0
                    params = None

                    while True:
                        if offset > 0:
                            params = dict()
                            params['offset'] = offset
                        response = self.request.get(target=request_target, parameters=params)

                        if response.status == 200:
                            data = response.data
                            mr_data = data['MRData']
                            race_table = mr_data['RaceTable']
                            races = race_table['Races']
                            for race in races:
                                for race_lap in race['Laps']:
                                    for timing in race_lap['Timings']:
                                        lap_row = LapTime()
                                        lap_row.driver_id = timing['driverId']
                                        lap_row.position = timing['position']
                                        lap_row.time = timing['time']
                                        if timing.get('time') and timing.get('time') != '':
                                            try:
                                                lap_time = datetime.strptime(timing['time'], '%M:%S.%f').time()
                                                lap_row.milliseconds = (lap_time.minute * 60000) + (
                                                        lap_time.second * 1000) + (lap_time.microsecond / 1000)
                                            except ValueError as error:
                                                lap_time = datetime.strptime(timing['time'], '%H:%M:%S.%f').time()
                                                lap_row.milliseconds = (lap_time.hour * 3600000) + (
                                                        lap_time.minute * 60000) + (lap_time.second * 1000) + (
                                                                               lap_time.microsecond / 1000)
                                        lap_row.lap = race_lap['number']
                                        lap_row.year = race['season']
                                        lap_row.round = race['round']

                                        self.laps_data.append(lap_row)

                            total_rows = int(mr_data.get('total', 0))
                            batch_size = int(mr_data.get('limit', 0))
                            offset = offset + batch_size

                            if offset > total_rows:
                                if offset:
                                    current_round += 1
                                if total_rows == 0:
                                    current_round = None
                                    max_round = None
                                break

    def _store_laps_data(self):
        """
            Store pit stops data
        :return: The query response
        """
        return self.azure_db.insert(table_name='lap_times', data=self.laps_data)


class ResultsCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'results.json'
        self.results_data = list()

    def run(self):
        self._populate_env()
        self._collect_results_data()
        if self.results_data:
            response = self._store_results_data()
            print(f'Results rows: {response}')
        else:
            print(f'No new laps data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['max_results_date'] = dict()

        # Collect results information
        results_query = """
            SELECT max_year.max_year,
                   MAX(results.round) AS max_round
            FROM results
                     INNER JOIN (SELECT MAX(results.year) AS max_year
                                 FROM results) AS max_year ON max_year.max_year = results.year
            WHERE results.year = max_year.max_year
            GROUP BY max_year.max_year;
        """
        results_raw = self.azure_db.select(query=results_query)

        for row in results_raw:
            self.env['max_results_date'] = {'year': int(row['max_year']),
                                            'round': int(row['max_round'])}

    def _collect_results_data(self):
        # if self.env['max_results_date']:
        max_year = self.env['max_results_date']['year']
        max_round = self.env['max_results_date']['round'] + 1
        for year in range(max_year, datetime.now().year + 1):
            current_round = max_round if max_round else 1

            while current_round:
                request_target = f'{year}/{current_round}/{self.target}'
                offset = 0
                params = None

                while True:
                    if offset:
                        params = dict()
                        params['offset'] = 1000
                    response = self.request.get(target=request_target, parameters=params)

                    if response.status == 200:
                        data = response.data
                        mr_data = data['MRData']
                        race_table = mr_data['RaceTable']
                        races = race_table['Races']
                        if races:
                            current_round += 1
                        else:
                            current_round = None
                            max_round = None
                        for race in races:
                            results = race['Results']
                            for result in results:
                                result_row = Result()
                                result_row.driver_id = result['Driver']['driverId']
                                result_row.constructor_id = result['Constructor']['constructorId']
                                result_row.number = result['number']
                                result_row.grid = result['grid']
                                result_row.position = result['position']
                                result_row.points = result['points']
                                result_row.laps = result['laps']
                                result_row.status_id = result['status']

                                if result.get('Time'):
                                    result_row.milliseconds = int(result['Time']['millis'])
                                if result.get('FastestLap'):
                                    result_row.fastest_lap = result['FastestLap']['lap']
                                    result_row.rank = result['FastestLap']['rank']
                                    result_row.fastest_lap_speed = result['FastestLap']['AverageSpeed']['speed']

                                    fastest_lap_time = datetime.strptime(result['FastestLap']['Time']['time'],
                                                                         '%M:%S.%f').time()
                                    result_row.fastest_lap_time = (fastest_lap_time.minute * 60000) + (
                                            fastest_lap_time.second * 1000) + (fastest_lap_time.microsecond / 1000)

                                result_row.year = race_table['season']
                                result_row.round = race_table['round']

                                self.results_data.append(result_row)

                        total_rows = int(mr_data.get('total', 0))
                        batch_size = int(mr_data.get('limit', 0))
                        offset = offset + batch_size
                        if offset > total_rows:
                            break

    def _store_results_data(self):
        """
            Store pit stops data
        :return: The query response
        """
        return self.azure_db.insert(table_name='results', data=self.results_data)


class SprintResultsCollector(F1DataCollector):
    def __init__(self):
        super().__init__()

        self.target = 'sprint.json'
        self.sprint_results_data = list()

    def run(self):
        self._populate_env()
        self._collect_sprint_results_data()
        if self.sprint_results_data:
            response = self._store_sprint_results_data()
            print(f'Sprint results rows: {response}')
        else:
            print(f'No new sprint results data')

    def _populate_env(self):
        """Populate the task environment data"""
        self.env['max_sprint_results_date'] = dict()

        # Collect sprint results information
        sprint_results_query = """
            SELECT max_year.max_year,
                   MAX(sprint_results.round) AS max_round
            FROM sprint_results
                     INNER JOIN (SELECT MAX(sprint_results.year) AS max_year
                                 FROM sprint_results) AS max_year ON max_year.max_year = sprint_results.year
            WHERE sprint_results.year = max_year.max_year
            GROUP BY max_year.max_year;
        """
        sprint_results_raw = self.azure_db.select(query=sprint_results_query)

        for row in sprint_results_raw:
            self.env['max_sprint_results_date'] = {'year': int(row['max_year']),
                                                   'round': int(row['max_round'])}

    def _collect_sprint_results_data(self, target=None):
        if self.env['max_sprint_results_date']:
            max_year = self.env['max_sprint_results_date']['year']
            max_round = self.env['max_sprint_results_date']['round'] + 1
            for year in range(max_year, datetime.now().year + 1):
                current_round = max_round if max_round else 1

                while current_round:
                    request_target = f'{year}/{current_round}/{self.target}'
                    offset = 0
                    params = None

                    while True:
                        if offset:
                            params = dict()
                            params['offset'] = 1000
                        response = self.request.get(target=request_target, parameters=params)

                        if response.status == 200:
                            data = response.data
                            mr_data = data['MRData']
                            race_table = mr_data['RaceTable']
                            races = race_table['Races']
                            if races:
                                current_round += 1
                            else:
                                current_round = None
                                max_round = None
                            for race in races:
                                results = race['SprintResults']
                                for result in results:
                                    result_row = SprintResult()
                                    result_row.driver_id = result['Driver']['driverId']
                                    result_row.constructor_id = result['Constructor']['constructorId']
                                    result_row.number = result['number']
                                    result_row.grid = result['grid']
                                    result_row.position = result['position']
                                    result_row.points = result['points']
                                    result_row.laps = result['laps']
                                    result_row.status_id = result['status']

                                    if result.get('Time'):
                                        result_row.milliseconds = int(result['Time']['millis'])
                                    if result.get('FastestLap'):
                                        result_row.fastest_lap = result['FastestLap']['lap']

                                        fastest_lap_time = datetime.strptime(result['FastestLap']['Time']['time'],
                                                                             '%M:%S.%f').time()
                                        result_row.fastest_lap_time = (fastest_lap_time.minute * 60000) + (
                                                fastest_lap_time.second * 1000) + (fastest_lap_time.microsecond / 1000)

                                    result_row.year = race_table['season']
                                    result_row.round = race_table['round']
                                    self.sprint_results_data.append(result_row)

                            total_rows = int(mr_data.get('total', 0))
                            batch_size = int(mr_data.get('limit', 0))
                            offset = offset + batch_size
                            if offset > total_rows:
                                break

    def _store_sprint_results_data(self):
        """
            Store pit stops data
        :return: The query response
        """
        return self.azure_db.insert(table_name='sprint_results', data=self.sprint_results_data)


if __name__ == '__main__':
    seasons_data = SeasonCollector()
    seasons_data.run()
    drivers_data = DriverCollector()
    drivers_data.run()
    constructors_data = ConstructorCollector()
    constructors_data.run()
    status_data = StatusCollector()
    status_data.run()
    circuits_data = CircuitCollector()
    circuits_data.run()
    races_data = RaceCollector()
    races_data.run()
    driver_standings_data = DriverStandingsCollector()
    driver_standings_data.run()
    constructor_standings_data = ConstructorStandingsCollector()
    constructor_standings_data.run()
    qualifying_data = QualifyingCollector()
    qualifying_data.run()
    pit_stops_data = PitStopsCollector()
    pit_stops_data.run()
    laps_data = LapTimesCollector()
    laps_data.run()
    results_data = ResultsCollector()
    results_data.run()
    sprint_results_data = SprintResultsCollector()
    sprint_results_data.run()
