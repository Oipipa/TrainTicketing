from traits.implementation import Traits, TraitsUtility
from public.traits.interface import *
import pytest


def test_add_and_fetch_user(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    utils = TraitsUtility(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "testuser@example.com"
    user_details = "Test User Details"

    # Add the user
    t.add_user(user_email, user_details)

    # Fetch all users
    users = utils.get_all_users()

    # Verify the user is added
    assert user_email in users, "User was not added correctly"


def test_add_and_delete_user(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)
    utils = TraitsUtility(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "deletetestuser@example.com"
    user_details = "Test User Details"

    # Add the user
    t.add_user(user_email, user_details)

    # Delete the user
    t.delete_user(user_email)

    # Fetch all users
    users = utils.get_all_users()

    # Verify the user is deleted
    assert user_email not in users, "User was not deleted correctly"


def test_add_and_fetch_train(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_1")
    train_capacity = 150
    train_status = TrainStatus.OPERATIONAL

    # Add the train
    t.add_train(train_key, train_capacity, train_status)

    # Fetch the train status
    fetched_status = t.get_train_current_status(train_key)

    # Verify the train status
    assert fetched_status == train_status, "Train status does not match"


def test_add_train_with_invalid_data(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_invalid")
    train_capacity = 150
    train_status = TrainStatus.OPERATIONAL

    # Add the train
    t.add_train(train_key, train_capacity, train_status)

    # Try adding the same train again, should raise a ValueError
    with pytest.raises(ValueError):
        t.add_train(train_key, train_capacity, train_status)


def test_add_and_connect_train_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("station_1")
    station_key_2 = TraitsKey("station_2")
    station_details = "Station Details"

    # Add train stations
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)

    # Connect the train stations
    travel_time = 30  # in minutes
    t.connect_train_stations(station_key_1, station_key_2, travel_time)

    # Verify connection by fetching the connected stations
    with t.neo4j_driver.session() as session:
        result = session.run("MATCH (s1:Station {id: $id1})-[:CONNECTED_TO]->(s2:Station {id: $id2}) RETURN s1, s2",
                             id1=station_key_1.id, id2=station_key_2.id)
        connection = result.single()
        assert connection is not None, "Stations were not connected correctly"


def test_search_connections_no_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    start_station = TraitsKey("non_existing_start")
    end_station = TraitsKey("non_existing_end")

    with pytest.raises(ValueError):
        t.search_connections(start_station, end_station)


def test_buy_ticket_non_existing_user(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "non_existing_user@example.com"
    connection = {
        'train_id': 'test_train',
        'departure_time': '2024-01-01 08:00:00'
    }

    with pytest.raises(ValueError):
        t.buy_ticket(user_email, connection)


def test_get_purchase_history_non_existing_user(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "non_existing_user@example.com"

    history = t.get_purchase_history(user_email)
    assert len(history) == 0, "Purchase history should be empty for non-existing user"


def test_update_train_details_partial(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_update_partial")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Update train details
    new_train_capacity = 120
    t.update_train_details(train_key, train_capacity=new_train_capacity)

    # Fetch updated train status
    updated_status = t.get_train_current_status(train_key)
    assert updated_status == train_status, "Train status should not have changed"

    # Verify updated capacity in RDBMS
    cursor = rdbms_connection.cursor()
    cursor.execute("SELECT capacity FROM trains WHERE id = %s", (train_key.id,))
    updated_capacity = cursor.fetchone()[0]
    assert updated_capacity == new_train_capacity, "Train capacity was not updated correctly"


def test_delete_train_and_verify_cascading_deletes(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_delete")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Delete train
    t.delete_train(train_key)

    # Verify train is deleted in RDBMS
    cursor = rdbms_connection.cursor()
    cursor.execute("SELECT * FROM trains WHERE id = %s", (train_key.id,))
    train = cursor.fetchone()
    assert train is None, "Train was not deleted from RDBMS"

    # Verify train is deleted in Neo4j
    with t.neo4j_driver.session() as session:
        result = session.run("MATCH (t:Train {id: $train_id}) RETURN t", train_id=train_key.id)
        train_node = result.single()
        assert train_node is None, "Train was not deleted from Neo4j"


def test_update_train_status_delayed_and_broken(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_status_update")
    train_capacity = 200
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Update train status to DELAYED
    t.update_train_details(train_key, train_status=TrainStatus.DELAYED)
    assert t.get_train_current_status(train_key) == TrainStatus.DELAYED, "Train status was not updated to DELAYED"

    # Update train status to BROKEN
    t.update_train_details(train_key, train_status=TrainStatus.BROKEN)
    assert t.get_train_current_status(train_key) == TrainStatus.BROKEN, "Train status was not updated to BROKEN"


def test_connect_train_stations_invalid_travel_time(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("station_1_invalid")
    station_key_2 = TraitsKey("station_2_invalid")
    station_details = "Station Details"

    # Add train stations
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)

    # Attempt to connect the train stations with invalid travel time
    invalid_travel_time = -10  # Invalid travel time
    with pytest.raises(ValueError):
        t.connect_train_stations(station_key_1, station_key_2, invalid_travel_time)


def test_add_schedule_with_one_stop(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_one_stop")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Add one station
    station_key = TraitsKey("station_one_stop")
    station_details = "Station Details"
    t.add_train_station(station_key, station_details)

    # Attempt to add a schedule with only one stop
    stops = [(station_key, 5)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    with pytest.raises(ValueError):
        t.add_schedule(
            train_key,
            starting_hours_24_h, starting_minutes,
            stops,
            valid_from_day, valid_from_month, valid_from_year,
            valid_until_day, valid_until_month, valid_until_year
        )


def test_search_connections_with_existing_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    # Add stations
    start_station_key = TraitsKey("start_station_search")
    end_station_key = TraitsKey("end_station_search")
    station_details = "Station Details"
    t.add_train_station(start_station_key, station_details)
    t.add_train_station(end_station_key, station_details)

    # Connect stations
    travel_time = 15  # in minutes
    t.connect_train_stations(start_station_key, end_station_key, travel_time)

    # Search for connections
    connections = t.search_connections(start_station_key, end_station_key)

    assert len(connections) > 0, "Connections were not found between existing stations"


def test_buy_ticket_and_reserve_seats(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    # Add user
    user_email = "ticketuser@example.com"
    user_details = "Ticket User Details"
    t.add_user(user_email, user_details)

    # Add train
    train_key = TraitsKey("test_train_ticket")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    t.add_train(train_key, train_capacity, train_status)

    # Add and connect stations
    start_station_key = TraitsKey("start_station_ticket")
    end_station_key = TraitsKey("end_station_ticket")
    station_details = "Station Details"
    t.add_train_station(start_station_key, station_details)
    t.add_train_station(end_station_key, station_details)
    t.connect_train_stations(start_station_key, end_station_key, 15)

    # Add schedule
    stops = [(start_station_key, 5), (end_station_key, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024
    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Buy ticket
    connection = {'train_id': train_key.id, 'departure_time': '2024-01-01 08:00:00'}
    t.buy_ticket(user_email, connection, also_reserve_seats=True)

    # Check purchase history
    history = t.get_purchase_history(user_email)
    assert len(history) == 1, "Ticket purchase was not recorded in the history"


def test_connect_stations_already_connected(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("station_already_connected_1")
    station_key_2 = TraitsKey("station_already_connected_2")
    station_details = "Station Details"

    # Add train stations
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)

    # Connect the train stations
    travel_time = 30  # in minutes
    t.connect_train_stations(station_key_1, station_key_2, travel_time)

    # Attempt to connect the same stations again
    with pytest.raises(ValueError):
        t.connect_train_stations(station_key_1, station_key_2, travel_time)


def test_delete_non_existing_train(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("non_existing_train")

    # Attempt to delete a non-existing train
    t.delete_train(train_key)

    # Verify that no exception is raised and operation completes
    assert True, "Deleting a non-existing train should not raise an error"


def test_update_train_details_invalid_capacity(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_invalid_capacity")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Attempt to update train with invalid capacity
    invalid_capacity = -50
    with pytest.raises(ValueError):
        t.update_train_details(train_key, train_capacity=invalid_capacity)


def test_add_and_retrieve_users(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "newuser@example.com"
    user_details = "New User Details"

    # Add user
    t.add_user(user_email, user_details)

    # Retrieve users
    users = t.get_all_users()
    assert user_email in users, "User should be added to the database"

    # Cleanup
    t.delete_user(user_email)

def test_add_and_retrieve_trains(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_retrieve")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Retrieve train status
    status = t.get_train_current_status(train_key)
    assert status == train_status, "Train should be added with correct status"

    # Cleanup
    t.delete_train(train_key)


def test_update_train_capacity_and_status(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("test_train_update")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Update train capacity and status
    new_capacity = 150
    new_status = TrainStatus.DELAYED
    t.update_train_details(train_key, train_capacity=new_capacity, train_status=new_status)

    # Retrieve updated train details
    status = t.get_train_current_status(train_key)
    cursor = rdbms_connection.cursor()
    cursor.execute("SELECT capacity FROM trains WHERE id = %s", (train_key.id,))
    capacity = cursor.fetchone()[0]
    assert status == new_status, "Train status should be updated"
    assert capacity == new_capacity, "Train capacity should be updated"

    # Cleanup
    t.delete_train(train_key)


def test_add_and_connect_train_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("station_1")
    station_key_2 = TraitsKey("station_2")
    station_details = "Station Details"

    # Add stations
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)

    # Connect stations
    travel_time = 15
    t.connect_train_stations(station_key_1, station_key_2, travel_time)

    # Verify connection
    with neo4j_db.session() as session:
        result = session.run("MATCH (s1:Station {id: $start_id})-[:CONNECTED_TO]->(s2:Station {id: $end_id}) RETURN s1, s2",
                             start_id=station_key_1.id, end_id=station_key_2.id)
        assert result.single() is not None, "Stations should be connected"

    # Cleanup
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s)", (station_key_1.id, station_key_2.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2] DETACH DELETE s",
                    id1=station_key_1.id, id2=station_key_2.id)

def test_search_connections(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("station_search_1")
    station_key_2 = TraitsKey("station_search_2")
    station_details = "Station Details"
    train_key = TraitsKey("train_search")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    travel_time = 15

    # Add stations and train
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)
    t.add_train(train_key, train_capacity, train_status)

    # Connect stations
    t.connect_train_stations(station_key_1, station_key_2, travel_time)

    # Add schedule
    stops = [(station_key_1, 5), (station_key_2, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024
    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Search for connections
    connections = t.search_connections(station_key_1, station_key_2)
    assert len(connections) > 0, "There should be at least one connection"
    print(f"Found connections: {connections}")

    # Cleanup
    t.delete_train(train_key)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s)", (station_key_1.id, station_key_2.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2] DETACH DELETE s",
                    id1=station_key_1.id, id2=station_key_2.id)
        session.run("MATCH (t:Train {id: $train_id}) DETACH DELETE t", train_id=train_key.id)


def test_add_user_invalid_email(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    invalid_user_email = "invalid-email"
    user_details = "Invalid Email User Details"

    try:
        t.add_user(invalid_user_email, user_details)
    except ValueError as e:
        assert str(e) == "Invalid email address", "Should raise ValueError for invalid email"


def test_add_duplicate_train_station(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key = TraitsKey("duplicate_station")
    station_details = "Station Details"

    # Add station
    t.add_train_station(station_key, station_details)

    # Try adding the same station again
    try:
        t.add_train_station(station_key, station_details)
    except ValueError as e:
        assert str(e) == "Station already exists", "Should raise ValueError for duplicate station"

    # Cleanup
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id = %s", (station_key.id,))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station {id: $station_id}) DETACH DELETE s", station_id=station_key.id)


def test_update_train_status_only(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_update_status_only")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Update train status only
    new_status = TrainStatus.DELAYED
    t.update_train_details(train_key, train_status=new_status)

    # Retrieve updated train status
    status = t.get_train_current_status(train_key)
    assert status == new_status, "Train status should be updated"

    # Cleanup
    t.delete_train(train_key)


def test_connect_nonexistent_train_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("nonexistent_station_1")
    station_key_2 = TraitsKey("nonexistent_station_2")
    travel_time = 15

    try:
        t.connect_train_stations(station_key_1, station_key_2, travel_time)
    except ValueError as e:
        assert str(e) == "One or both stations do not exist", "Should raise ValueError for non-existent stations"


def test_add_schedule_with_invalid_stops(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_invalid_schedule")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    station_key_1 = TraitsKey("station_invalid_1")
    station_key_2 = TraitsKey("station_invalid_2")
    station_details = "Station Details"
    travel_time = 15

    # Add train and stations
    t.add_train(train_key, train_capacity, train_status)
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)

    # Do not connect the stations
    stops = [(station_key_1, 5), (station_key_2, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    try:
        t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)
    except ValueError as e:
        assert str(e).startswith("Stations"), "Should raise ValueError for invalid stops"

    # Cleanup
    t.delete_train(train_key)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s)", (station_key_1.id, station_key_2.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2] DETACH DELETE s",
                    id1=station_key_1.id, id2=station_key_2.id)


def test_add_user_with_long_email(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    long_email = "user_with_an_exceptionally_long_email_address_that_exceeds_typical_length_limits@example.com"
    user_details = "User with Long Email"

    t.add_user(long_email, user_details)

    # Verify user is added
    users = t.get_all_users()
    assert long_email in users, "User with long email should be added to the database"

    # Cleanup
    t.delete_user(long_email)


def test_add_train_with_zero_capacity(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_zero_capacity")
    train_capacity = 0
    train_status = TrainStatus.OPERATIONAL

    try:
        t.add_train(train_key, train_capacity, train_status)
    except ValueError as e:
        assert str(e) == "Invalid train capacity", "Should raise ValueError for zero capacity"

    # Cleanup in case of failure
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM trains WHERE id = %s", (train_key.id,))
    rdbms_admin_connection.commit()


def test_add_schedule_with_overlapping_validity_periods(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_overlap_schedule")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    station_key_1 = TraitsKey("station_overlap_1")
    station_key_2 = TraitsKey("station_overlap_2")
    station_details = "Station Details"
    travel_time = 15

    # Add train and stations
    t.add_train(train_key, train_capacity, train_status)
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)
    t.connect_train_stations(station_key_1, station_key_2, travel_time)

    # Add first schedule
    stops = [(station_key_1, 5), (station_key_2, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 30, 6, 2024
    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Try adding overlapping schedule
    valid_from_day, valid_from_month, valid_from_year = 1, 5, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    try:
        t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)
    except ValueError as e:
        assert "overlapping" in str(e).lower(), "Should raise ValueError for overlapping schedules"

    # Cleanup
    t.delete_train(train_key)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s)", (station_key_1.id, station_key_2.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2] DETACH DELETE s",
                    id1=station_key_1.id, id2=station_key_2.id)
        session.run("MATCH (t:Train {id: $train_id}) DETACH DELETE t", train_id=train_key.id)


def test_connect_stations_with_zero_travel_time(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("station_zero_time_1")
    station_key_2 = TraitsKey("station_zero_time_2")
    station_details = "Station Details"
    travel_time = 0

    # Add stations
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)

    # Try connecting stations with zero travel time
    try:
        t.connect_train_stations(station_key_1, station_key_2, travel_time)
    except ValueError as e:
        assert str(e) == "Invalid travel time", "Should raise ValueError for zero travel time"

    # Cleanup
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s)", (station_key_1.id, station_key_2.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2] DETACH DELETE s",
                    id1=station_key_1.id, id2=station_key_2.id)


def test_purchase_tickets_for_multiple_trains(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "multi_train_user@example.com"
    user_details = "Multi Train User Details"
    t.add_user(user_email, user_details)

    # Add trains
    train_key_1 = TraitsKey("train_multi_1")
    train_key_2 = TraitsKey("train_multi_2")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    t.add_train(train_key_1, train_capacity, train_status)
    t.add_train(train_key_2, train_capacity, train_status)

    # Add and connect stations
    station_key_1 = TraitsKey("station_multi_1")
    station_key_2 = TraitsKey("station_multi_2")
    station_key_3 = TraitsKey("station_multi_3")
    station_details = "Station Details"
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)
    t.add_train_station(station_key_3, station_details)
    t.connect_train_stations(station_key_1, station_key_2, 10)
    t.connect_train_stations(station_key_2, station_key_3, 15)

    # Add schedules
    stops_1 = [(station_key_1, 5), (station_key_2, 10)]
    stops_2 = [(station_key_2, 5), (station_key_3, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    t.add_schedule(train_key_1, starting_hours_24_h, starting_minutes, stops_1, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)
    t.add_schedule(train_key_2, starting_hours_24_h, starting_minutes, stops_2, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Purchase tickets
    connection_1 = {'train_id': train_key_1.id, 'departure_time': '2024-01-01 08:00:00'}
    connection_2 = {'train_id': train_key_2.id, 'departure_time': '2024-01-01 08:15:00'}

    t.buy_ticket(user_email, connection_1, also_reserve_seats=True)
    t.buy_ticket(user_email, connection_2, also_reserve_seats=True)

    # Verify purchase history
    history = t.get_purchase_history(user_email)
    assert len(history) == 2, "User should have two tickets in their purchase history"

    # Cleanup
    t.delete_train(train_key_1)
    t.delete_train(train_key_2)
    t.delete_user(user_email)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s, %s)", (station_key_1.id, station_key_2.id, station_key_3.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2, $id3] DETACH DELETE s", id1=station_key_1.id, id2=station_key_2.id, id3=station_key_3.id)


def test_handle_train_delays_and_update_status(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_delay_test")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add train
    t.add_train(train_key, train_capacity, train_status)

    # Update train status to DELAYED
    new_status = TrainStatus.DELAYED
    t.update_train_details(train_key, train_status=new_status)

    # Retrieve updated train status
    status = t.get_train_current_status(train_key)
    assert status == new_status, "Train status should be updated to DELAYED"

    # Cleanup
    t.delete_train(train_key)


def test_retrieve_schedule_for_specific_train(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_schedule_retrieve")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    station_key_1 = TraitsKey("station_sched_1")
    station_key_2 = TraitsKey("station_sched_2")
    station_details = "Station Details"
    travel_time = 15

    # Add train and stations
    t.add_train(train_key, train_capacity, train_status)
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)
    t.connect_train_stations(station_key_1, station_key_2, travel_time)

    # Add schedule
    stops = [(station_key_1, 5), (station_key_2, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Retrieve schedule
    schedules = t.get_all_schedules()
    assert len(schedules) > 0, "There should be at least one schedule"

    # Cleanup
    t.delete_train(train_key)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s)", (station_key_1.id, station_key_2.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2] DETACH DELETE s", id1=station_key_1.id, id2=station_key_2.id)


def test_search_connections_with_multiple_criteria(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key_1 = TraitsKey("station_search_multi_1")
    station_key_2 = TraitsKey("station_search_multi_2")
    station_key_3 = TraitsKey("station_search_multi_3")
    station_details = "Station Details"
    travel_time_1 = 10
    travel_time_2 = 20
    train_key_1 = TraitsKey("train_search_multi_1")
    train_key_2 = TraitsKey("train_search_multi_2")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL

    # Add trains and stations
    t.add_train(train_key_1, train_capacity, train_status)
    t.add_train(train_key_2, train_capacity, train_status)
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)
    t.add_train_station(station_key_3, station_details)
    t.connect_train_stations(station_key_1, station_key_2, travel_time_1)
    t.connect_train_stations(station_key_2, station_key_3, travel_time_2)

    # Add schedules
    stops_1 = [(station_key_1, 5), (station_key_2, 10)]
    stops_2 = [(station_key_2, 5), (station_key_3, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    t.add_schedule(train_key_1, starting_hours_24_h, starting_minutes, stops_1, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)
    t.add_schedule(train_key_2, starting_hours_24_h, starting_minutes, stops_2, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Search for connections
    connections = t.search_connections(station_key_1, station_key_3, sort_by=SortingCriteria.OVERALL_TRAVEL_TIME)
    assert len(connections) > 0, "There should be at least one connection"

    # Cleanup
    t.delete_train(train_key_1)
    t.delete_train(train_key_2)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s, %s)", (station_key_1.id, station_key_2.id, station_key_3.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2, $id3] DETACH DELETE s", id1=station_key_1.id, id2=station_key_2.id, id3=station_key_3.id)


def test_add_and_retrieve_multiple_schedules(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_multiple_schedules")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    station_key_1 = TraitsKey("station_multi_sched_1")
    station_key_2 = TraitsKey("station_multi_sched_2")
    station_key_3 = TraitsKey("station_multi_sched_3")
    station_details = "Station Details"
    travel_time = 15

    # Add train and stations
    t.add_train(train_key, train_capacity, train_status)
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)
    t.add_train_station(station_key_3, station_details)
    t.connect_train_stations(station_key_1, station_key_2, travel_time)
    t.connect_train_stations(station_key_2, station_key_3, travel_time)

    # Add schedules
    stops_1 = [(station_key_1, 5), (station_key_2, 10)]
    stops_2 = [(station_key_2, 5), (station_key_3, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops_1, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)
    t.add_schedule(train_key, starting_hours_24_h + 2, starting_minutes, stops_2, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Retrieve schedules
    schedules = t.get_all_schedules()
    assert len(schedules) == 2, "There should be two schedules"

    # Cleanup
    t.delete_train(train_key)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s, %s)", (station_key_1.id, station_key_2.id, station_key_3.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2, $id3] DETACH DELETE s", id1=station_key_1.id, id2=station_key_2.id, id3=station_key_3.id)


def test_add_and_retrieve_multiple_schedules(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_multiple_schedules")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    station_key_1 = TraitsKey("station_multi_sched_1")
    station_key_2 = TraitsKey("station_multi_sched_2")
    station_key_3 = TraitsKey("station_multi_sched_3")
    station_details = "Station Details"
    travel_time = 15

    # Add train and stations
    t.add_train(train_key, train_capacity, train_status)
    t.add_train_station(station_key_1, station_details)
    t.add_train_station(station_key_2, station_details)
    t.add_train_station(station_key_3, station_details)
    t.connect_train_stations(station_key_1, station_key_2, travel_time)
    t.connect_train_stations(station_key_2, station_key_3, travel_time)

    # Add schedules
    stops_1 = [(station_key_1, 5), (station_key_2, 10)]
    stops_2 = [(station_key_2, 5), (station_key_3, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops_1, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)
    t.add_schedule(train_key, starting_hours_24_h + 2, starting_minutes, stops_2, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Retrieve schedules
    schedules = t.get_all_schedules()
    assert len(schedules) == 2, "There should be two schedules"

    # Cleanup
    t.delete_train(train_key)
    cursor = rdbms_admin_connection.cursor()
    cursor.execute("DELETE FROM stations WHERE id IN (%s, %s, %s)", (station_key_1.id, station_key_2.id, station_key_3.id))
    rdbms_admin_connection.commit()
    with neo4j_db.session() as session:
        session.run("MATCH (s:Station) WHERE s.id IN [$id1, $id2, $id3] DETACH DELETE s", id1=station_key_1.id, id2=station_key_2.id, id3=station_key_3.id)


def test_empty_purchase_history_for_new_user(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "newuser@example.com"
    user_details = "New User Details"
    t.add_user(user_email, user_details)

    history = t.get_purchase_history(user_email)
    assert len(history) == 0, "New user should have empty purchase history"

    # Cleanup
    t.delete_user(user_email)


def test_update_train_capacity_to_invalid_value(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    train_key = TraitsKey("train_invalid_capacity")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    t.add_train(train_key, train_capacity, train_status)

    with pytest.raises(ValueError) as exc_info:
        t.update_train_details(train_key, train_capacity=-10)
    assert "Invalid train capacity" in str(exc_info.value), "Should raise error for invalid train capacity"

    # Cleanup
    t.delete_train(train_key)


def test_buy_ticket_without_seat_reservation(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    # Add user
    user_email = "noreseatsuser@example.com"
    user_details = "No Reserve Seats User Details"
    t.add_user(user_email, user_details)

    # Add train
    train_key = TraitsKey("test_train_no_seats")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    t.add_train(train_key, train_capacity, train_status)

    # Add and connect stations
    start_station_key = TraitsKey("start_station_no_seats")
    end_station_key = TraitsKey("end_station_no_seats")
    station_details = "Station Details"
    t.add_train_station(start_station_key, station_details)
    t.add_train_station(end_station_key, station_details)
    t.connect_train_stations(start_station_key, end_station_key, 15)

    # Add schedule
    stops = [(start_station_key, 5), (end_station_key, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024
    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Buy a ticket without reserving a seat
    connection = {'train_id': train_key.id, 'departure_time': '2024-01-01 08:00:00'}
    t.buy_ticket(user_email, connection, also_reserve_seats=False)

    # Verify purchase
    purchases = t.get_purchase_history(user_email)
    assert len(purchases) == 1, "There should be one purchase"
    assert purchases[0][1] == train_key.id, "The train ID should match"
    assert purchases[0][2].strftime('%Y-%m-%d %H:%M:%S') == '2024-01-01 08:00:00', "The departure time should match"



def test_add_and_connect_multiple_stations(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    # Add and connect multiple stations
    station_keys = [TraitsKey(f"station_{i}") for i in range(5)]
    station_details = "Station Details"
    for key in station_keys:
        t.add_train_station(key, station_details)

    travel_time = 10
    for i in range(len(station_keys) - 1):
        t.connect_train_stations(station_keys[i], station_keys[i + 1], travel_time)

    # Verify connections
    for i in range(len(station_keys) - 1):
        connections = t.search_connections(station_keys[i], station_keys[i + 1])
        assert len(
            connections) > 0, f"There should be a connection between {station_keys[i].id} and {station_keys[i + 1].id}"


def test_add_train_station_with_duplicate_key(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    station_key = TraitsKey("duplicate_station")
    station_details = "Station Details"
    t.add_train_station(station_key, station_details)

    with pytest.raises(ValueError) as exc_info:
        t.add_train_station(station_key, station_details)
    assert "Station already exists" in str(exc_info.value), "Should raise error for duplicate station key"


def test_user_purchase_history(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    user_email = "historyuser@example.com"
    user_details = "History User Details"
    t.add_user(user_email, user_details)

    # Add train
    train_key = TraitsKey("train_history")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    t.add_train(train_key, train_capacity, train_status)

    # Add and connect stations
    start_station_key = TraitsKey("start_station_history")
    end_station_key = TraitsKey("end_station_history")
    station_details = "Station Details"
    t.add_train_station(start_station_key, station_details)
    t.add_train_station(end_station_key, station_details)
    t.connect_train_stations(start_station_key, end_station_key, 15)

    # Add schedule
    stops = [(start_station_key, 5), (end_station_key, 10)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024
    t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    # Buy a ticket
    connection = {'train_id': train_key.id, 'departure_time': '2024-01-01 08:00:00'}
    t.buy_ticket(user_email, connection, also_reserve_seats=False)

    # Verify purchase history
    purchases = t.get_purchase_history(user_email)
    assert len(purchases) == 1, "There should be one purchase"
    assert purchases[0][1] == train_key.id, "The train ID should match"
    assert purchases[0][2].strftime('%Y-%m-%d %H:%M:%S') == '2024-01-01 08:00:00', "The departure time should match"


def test_add_schedule_with_unconnected_stops(rdbms_connection, rdbms_admin_connection, neo4j_db):
    t = Traits(rdbms_connection, rdbms_admin_connection, neo4j_db)

    # Add and connect stations
    start_station_key = TraitsKey("start_station_unconnected")
    middle_station_key = TraitsKey("middle_station_unconnected")
    end_station_key = TraitsKey("end_station_unconnected")
    station_details = "Station Details"
    t.add_train_station(start_station_key, station_details)
    t.add_train_station(middle_station_key, station_details)
    t.add_train_station(end_station_key, station_details)

    # Only connect start and middle station
    t.connect_train_stations(start_station_key, middle_station_key, 15)

    # Add train
    train_key = TraitsKey("test_train_unconnected")
    train_capacity = 100
    train_status = TrainStatus.OPERATIONAL
    t.add_train(train_key, train_capacity, train_status)

    # Attempt to add a schedule with unconnected stops
    stops = [(start_station_key, 5), (middle_station_key, 10), (end_station_key, 15)]
    starting_hours_24_h, starting_minutes = 8, 0
    valid_from_day, valid_from_month, valid_from_year = 1, 1, 2024
    valid_until_day, valid_until_month, valid_until_year = 31, 12, 2024

    with pytest.raises(ValueError) as exc_info:
        t.add_schedule(train_key, starting_hours_24_h, starting_minutes, stops, valid_from_day, valid_from_month, valid_from_year, valid_until_day, valid_until_month, valid_until_year)

    assert "Stations middle_station_unconnected and end_station_unconnected are not connected" in str(exc_info.value), "Should raise error for unconnected stops"
