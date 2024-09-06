from public.traits.interface import TraitsUtilityInterface, BASE_USER_NAME, BASE_USER_PASS, ADMIN_USER_NAME, ADMIN_USER_PASS
from typing import List, Optional, Tuple
from public.traits.interface import TraitsInterface, TraitsUtilityInterface, TraitsKey, TrainStatus, SortingCriteria
import mysql.connector
import json
import uuid


class TraitsUtility(TraitsUtilityInterface):

    def __init__(self, rdbms_connection, rdbms_admin_connection, neo4j_driver) -> None:
        self.rdbms_connection = rdbms_connection
        self.rdbms_admin_connection = rdbms_admin_connection
        self.neo4j_driver = neo4j_driver

    def set_transaction_isolation_level(self, connection, level='READ COMMITTED'):
        cursor = connection.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
        cursor.close()

    @staticmethod
    def generate_sql_initialization_code() -> List[str]:
        return [
            f"DROP USER IF EXISTS '{ADMIN_USER_NAME}'@'%';",
            f"DROP USER IF EXISTS '{BASE_USER_NAME}'@'%';",
            f"CREATE USER '{ADMIN_USER_NAME}'@'%' IDENTIFIED BY '{ADMIN_USER_PASS}';",
            f"CREATE USER '{BASE_USER_NAME}'@'%' IDENTIFIED BY '{BASE_USER_PASS}';",
            f"GRANT ALL PRIVILEGES ON *.* TO '{ADMIN_USER_NAME}'@'%' WITH GRANT OPTION;",
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO '{BASE_USER_NAME}'@'%';",
            f"FLUSH PRIVILEGES;",
            "CREATE TABLE IF NOT EXISTS users (email VARCHAR(255) PRIMARY KEY, details TEXT);",
            "CREATE TABLE IF NOT EXISTS trains (id VARCHAR(255) PRIMARY KEY, capacity INT, status VARCHAR(255), reserved_seats INT DEFAULT 0);",
            "CREATE TABLE IF NOT EXISTS stations (id VARCHAR(255) PRIMARY KEY, details TEXT);",
            "CREATE TABLE IF NOT EXISTS purchases (user_email VARCHAR(255), train_id VARCHAR(255), purchase_time DATETIME, FOREIGN KEY (user_email) REFERENCES users(email), FOREIGN KEY (train_id) REFERENCES trains(id));"
        ]

    def get_all_users(self) -> List[str]:
        self.set_transaction_isolation_level(self.rdbms_connection)
        cursor = self.rdbms_connection.cursor()
        query = "SELECT email FROM users"
        cursor.execute(query)
        users = cursor.fetchall()
        print(f"Users fetched from DB: {users}")  # Debugging statement
        cursor.close()
        return [user[0] for user in users]

    def get_all_schedules(self) -> List:
        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (s:Schedule) RETURN s")
            schedules = result.data()
        return schedules


class Traits(TraitsInterface):

    def __init__(self, rdbms_connection, rdbms_admin_connection, neo4j_driver) -> None:
        self.rdbms_connection = rdbms_connection
        self.rdbms_admin_connection = rdbms_admin_connection
        self.neo4j_driver = neo4j_driver
        self.last_train_key = None

    def get_all_schedules(self) -> List:
        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (s:Schedule) RETURN s")
            schedules = result.data()
        return schedules


    def set_transaction_isolation_level(self, connection, level='READ COMMITTED'):
        cursor = connection.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {level}")
        cursor.close()

    def search_connections(self, starting_station_key: TraitsKey, ending_station_key: TraitsKey,
                           travel_time_day: int = None, travel_time_month: int = None, travel_time_year: int = None,
                           is_departure_time=True, sort_by: SortingCriteria = SortingCriteria.OVERALL_TRAVEL_TIME,
                           is_ascending: bool = True, limit: int = 5) -> List:
        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (s:Station {id: $start_id}), (e:Station {id: $end_id}) RETURN s, e",
                                 start_id=starting_station_key.id, end_id=ending_station_key.id)
            if result.single() is None:
                raise ValueError("Starting or ending station does not exist")

            query = """
                MATCH (start:Station {id: $start_id})-[:CONNECTED_TO*]->(end:Station {id: $end_id})
                RETURN start, end
                ORDER BY start.travel_time ASC
                LIMIT $limit
            """
            connections = session.run(query, start_id=starting_station_key.id, end_id=ending_station_key.id,
                                      limit=limit)
            connections = [record for record in connections]

            return connections

    def get_all_users(self) -> List[str]:
        cursor = self.rdbms_connection.cursor()
        cursor.execute("SELECT email FROM users")
        users = cursor.fetchall()
        cursor.close()
        return [user[0] for user in users]

    def get_train_current_status(self, train_key: TraitsKey) -> Optional[TrainStatus]:
        self.set_transaction_isolation_level(self.rdbms_connection)  # Set isolation level

        cursor = self.rdbms_connection.cursor()
        query = "SELECT status FROM trains WHERE id = %s"
        cursor.execute(query, (train_key.id,))
        result = cursor.fetchone()
        cursor.close()
        if result:
            print(f"Fetched status from DB: {result[0]}")  # Debugging statement
            return TrainStatus[result[0].upper()]
        return None

    def buy_ticket(self, user_email: str, connection, also_reserve_seats=True):
        cursor = self.rdbms_admin_connection.cursor()

        # Check if the user exists
        query = "SELECT COUNT(*) FROM users WHERE email = %s"
        cursor.execute(query, (user_email,))
        if cursor.fetchone()[0] == 0:
            raise ValueError("User does not exist")

        # Check if the connection is valid (this part assumes the connection object contains the necessary details)
        if connection is None:
            raise ValueError("Invalid connection")

        train_id = connection['train_id']
        departure_time = connection['departure_time']

        query = "SELECT COUNT(*) FROM trains WHERE id = %s"
        cursor.execute(query, (train_id,))
        if cursor.fetchone()[0] == 0:
            raise ValueError("Train does not exist")

        # Book the ticket
        query = "INSERT INTO purchases (user_email, train_id, purchase_time) VALUES (%s, %s, %s)"
        cursor.execute(query, (user_email, train_id, departure_time))
        self.rdbms_admin_connection.commit()

        # Reserve seats if required
        if also_reserve_seats:
            query = "SELECT capacity FROM trains WHERE id = %s"
            cursor.execute(query, (train_id,))
            capacity = cursor.fetchone()[0]

            query = "SELECT COUNT(*) FROM purchases WHERE train_id = %s AND purchase_time = %s"
            cursor.execute(query, (train_id, departure_time))
            reserved_seats = cursor.fetchone()[0]

            if reserved_seats >= capacity:
                raise ValueError("No available seats")

            query = "UPDATE trains SET reserved_seats = reserved_seats + 1 WHERE id = %s"
            cursor.execute(query, (train_id,))
            self.rdbms_admin_connection.commit()

        # Log the purchase in Neo4j for additional operations (e.g., viewing history)
        with self.neo4j_driver.session() as session:
            session.run("MATCH (u:User {email: $email}), (t:Train {id: $train_id}) "
                        "CREATE (u)-[:BOOKED {time: $time, reserved_seat: $reserved_seat}]->(t)",
                        email=user_email, train_id=train_id, time=departure_time, reserved_seat=also_reserve_seats)

    def get_purchase_history(self, user_email: str) -> List:
        cursor = self.rdbms_admin_connection.cursor()
        query = "SELECT * FROM purchases WHERE user_email = %s ORDER BY purchase_time DESC"
        cursor.execute(query, (user_email,))
        return cursor.fetchall()

    def add_user(self, user_email: str, user_details) -> None:
        if "@" not in user_email or "." not in user_email.split("@")[1]:
            raise ValueError("Invalid email address")

        if user_details is None:
            user_details = ""  # Use an empty string as a default value

        cursor = self.rdbms_admin_connection.cursor()
        query = "INSERT INTO users (email, details) VALUES (%s, %s)"
        try:
            cursor.execute(query, (user_email, json.dumps(user_details)))
            self.rdbms_admin_connection.commit()
            print(f"User {user_email} inserted with details: {user_details}")  # Debugging statement
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_DUP_ENTRY:
                raise ValueError("User already exists")
            else:
                raise ValueError(f"Failed to add user: {err}")
        finally:
            cursor.close()

    def delete_user(self, user_email: str) -> None:
        cursor = self.rdbms_admin_connection.cursor()
        query = "DELETE FROM users WHERE email = %s"
        cursor.execute(query, (user_email,))
        self.rdbms_admin_connection.commit()

    def add_train(self, train_key: Optional[TraitsKey], train_capacity: int, train_status: TrainStatus) -> TraitsKey:
        if train_key is None or train_key.id is None:
            train_key = TraitsKey(str(uuid.uuid4()))  # Generate a unique key if train_key is None
            print(f"Generated new train key: {train_key.id}")  # Debugging statement

        cursor = self.rdbms_admin_connection.cursor()
        query = "INSERT INTO trains (id, capacity, status) VALUES (%s, %s, %s)"
        try:
            cursor.execute(query, (train_key.id, train_capacity, train_status.name))
            self.rdbms_admin_connection.commit()
            print(f"Train {train_key.id} inserted with capacity {train_capacity} and status {train_status.name}")  # Debugging statement
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_DUP_ENTRY:
                raise ValueError("Train already exists")
            else:
                raise ValueError(f"Failed to add train: {err}")
        finally:
            cursor.close()

        with self.neo4j_driver.session() as session:
            session.run("CREATE (t:Train {id: $train_id, capacity: $capacity, status: $status})",
                        train_id=train_key.id, capacity=train_capacity, status=train_status.name)

        self.last_train_key = train_key  # Update the last train key

        return train_key  # Ensure the generated train_key is returned

    def update_train_details(self, train_key: TraitsKey, train_capacity: Optional[int] = None,
                             train_status: Optional[TrainStatus] = None) -> None:
        cursor = self.rdbms_admin_connection.cursor()
        if train_capacity is not None:
            if train_capacity <= 0:
                raise ValueError("Invalid train capacity")
            query = "UPDATE trains SET capacity = %s WHERE id = %s"
            cursor.execute(query, (train_capacity, train_key.id))
        if train_status is not None:
            query = "UPDATE trains SET status = %s WHERE id = %s"
            cursor.execute(query, (train_status.name, train_key.id))
        self.rdbms_admin_connection.commit()
        cursor.close()

        with self.neo4j_driver.session() as session:
            if train_capacity is not None:
                session.run("MATCH (t:Train {id: $train_id}) SET t.capacity = $capacity",
                            train_id=train_key.id, capacity=train_capacity)
            if train_status is not None:
                session.run("MATCH (t:Train {id: $train_id}) SET t.status = $status",
                            train_id=train_key.id, status=train_status.name)

    def delete_train(self, train_key: TraitsKey) -> None:
        cursor = self.rdbms_admin_connection.cursor()

        # Log current state before deletion
        cursor.execute("SELECT * FROM trains WHERE id = %s", (train_key.id,))
        train_before_deletion = cursor.fetchone()
        print(f"Train before deletion: {train_before_deletion}")

        # Delete associated purchases
        query = "DELETE FROM purchases WHERE train_id = %s"
        cursor.execute(query, (train_key.id,))
        self.rdbms_admin_connection.commit()
        print(f"Deleted purchases for train {train_key.id}")

        # Delete the train
        query = "DELETE FROM trains WHERE id = %s"
        cursor.execute(query, (train_key.id,))
        self.rdbms_admin_connection.commit()

        # Log current state after deletion
        cursor.execute("SELECT * FROM trains WHERE id = %s", (train_key.id,))
        train_after_deletion = cursor.fetchone()
        print(f"Train after deletion: {train_after_deletion}")

        cursor.close()
        print(f"Deleted train {train_key.id} from RDBMS")

        with self.neo4j_driver.session() as session:
            # Delete the train node and any relationships in Neo4j
            session.run("MATCH (t:Train {id: $train_id}) DETACH DELETE t", train_id=train_key.id)
            print(f"Deleted train {train_key.id} from Neo4j")

    def add_train_station(self, train_station_key: TraitsKey, train_station_details) -> None:
        cursor = self.rdbms_admin_connection.cursor()
        query = "INSERT INTO stations (id, details) VALUES (%s, %s)"
        try:
            cursor.execute(query, (train_station_key.id, train_station_details))
            self.rdbms_admin_connection.commit()
        except mysql.connector.Error as err:
            if err.errno == mysql.connector.errorcode.ER_DUP_ENTRY:
                raise ValueError("Station already exists")
            else:
                raise ValueError(f"Failed to add station: {err}")

        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (s:Station {id: $station_id}) RETURN s", station_id=train_station_key.id)
            if result.single():
                raise ValueError("Station already exists")
            session.run("CREATE (s:Station {id: $station_id, details: $details})", station_id=train_station_key.id,
                        details=train_station_details)

    def connect_train_stations(self, starting_train_station_key: TraitsKey, ending_train_station_key: TraitsKey,
                               travel_time_in_minutes: int) -> None:
        if travel_time_in_minutes <= 0 or travel_time_in_minutes > 60:
            raise ValueError("Invalid travel time")
        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (start:Station {id: $start_id}), (end:Station {id: $end_id}) RETURN start, end",
                                 start_id=starting_train_station_key.id, end_id=ending_train_station_key.id)
            if not result.single():
                raise ValueError("One or both stations do not exist")

            # Check if the connection already exists
            result = session.run(
                "MATCH (start:Station {id: $start_id})-[:CONNECTED_TO]->(end:Station {id: $end_id}) RETURN start, end",
                start_id=starting_train_station_key.id, end_id=ending_train_station_key.id)
            if result.single():
                raise ValueError("Stations are already connected")

            session.run(
                "MATCH (start:Station {id: $start_id}), (end:Station {id: $end_id}) CREATE (start)-[:CONNECTED_TO {travel_time: $travel_time}]->(end)",
                start_id=starting_train_station_key.id, end_id=ending_train_station_key.id,
                travel_time=travel_time_in_minutes)

    def add_schedule(self, train_key: Optional[TraitsKey], starting_hours_24_h: int, starting_minutes: int,
                     stops: List[Tuple[TraitsKey, int]], valid_from_day: int, valid_from_month: int,
                     valid_from_year: int, valid_until_day: int, valid_until_month: int, valid_until_year: int) -> None:
        if train_key is None:
            train_key = self.last_train_key  # Use the last generated train key if train_key is None

        if train_key is None or train_key.id is None:
            raise ValueError("Train key cannot be None")

        if len(stops) < 2:
            raise ValueError("Schedule must have at least two stops")
        if starting_hours_24_h < 0 or starting_hours_24_h > 23 or starting_minutes < 0 or starting_minutes > 59:
            raise ValueError("Invalid start time")
        if not (1 <= valid_from_day <= 31) or not (1 <= valid_from_month <= 12) or valid_from_year < 0:
            raise ValueError("Invalid start date")
        if not (1 <= valid_until_day <= 31) or not (1 <= valid_until_month <= 12) or valid_until_year < 0:
            raise ValueError("Invalid end date")
        if (valid_from_year, valid_from_month, valid_from_day) > (valid_until_year, valid_until_month, valid_until_day):
            raise ValueError("End date must be after start date")

        with self.neo4j_driver.session() as session:
            result = session.run("MATCH (t:Train {id: $train_id}) RETURN t", train_id=train_key.id)
            if not result.single():
                raise ValueError("Train does not exist")

            for i in range(len(stops) - 1):
                result = session.run(
                    "MATCH (start:Station {id: $start_id})-[:CONNECTED_TO]->(end:Station {id: $end_id}) RETURN start, end",
                    start_id=stops[i][0].id, end_id=stops[i + 1][0].id)
                if not result.single():
                    raise ValueError(f"Stations {stops[i][0].id} and {stops[i + 1][0].id} are not connected")

            schedule_id = f"{train_key.id}-{starting_hours_24_h:02d}{starting_minutes:02d}-{valid_from_year:04d}{valid_from_month:02d}{valid_from_day:02d}-{valid_until_year:04d}{valid_until_month:02d}{valid_until_day:02d}"
            session.run(
                "CREATE (s:Schedule {id: $schedule_id, train_id: $train_id, start_time: $start_time, valid_from: $valid_from, valid_until: $valid_until})",
                schedule_id=schedule_id, train_id=train_key.id,
                start_time=f"{starting_hours_24_h:02d}:{starting_minutes:02d}",
                valid_from=f"{valid_from_year:04d}-{valid_from_month:02d}-{valid_from_day:02d}",
                valid_until=f"{valid_until_year:04d}-{valid_until_month:02d}-{valid_until_day:02d}")

            for stop in stops:
                session.run(
                    "MATCH (s:Schedule {id: $schedule_id}) CREATE (s)-[:STOPS_AT {wait_time: $wait_time}]->(:Station {id: $station_id})",
                    schedule_id=schedule_id, wait_time=stop[1], station_id=stop[0].id)
