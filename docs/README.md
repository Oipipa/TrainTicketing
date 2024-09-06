## Solution Overview

### Allocating the role of SQL and NoSQL

- **SQL (MySQL)**:
  - **Usage**: 
    - Managing user data (`users` table).
    - Storing train details (`trains` table).
    - Logging ticket purchases (`purchases` table).
  - **Reason**: 
    - SQL databases like MySQL are highly suitable for structured data requiring strict adherence to ACID properties. This ensures data integrity and supports complex transactional queries which are essential for reliable operations in a ticketing system.
  - **Examples**:
    - **User Management**: The `users` table is defined with columns for `email` and `details`, ensuring each user is uniquely identifiable by their email.
      ```sql
      CREATE TABLE users (
          email VARCHAR(255) PRIMARY KEY,
          details TEXT
      );
      ```
    - **Train Management**: The `trains` table stores train-specific data, ensuring efficient storage and retrieval of train information.
      ```sql
      CREATE TABLE trains (
          id VARCHAR(255) PRIMARY KEY,
          capacity INT,
          status VARCHAR(255)
      );
      ```
    - **Purchase Logging**: The `purchases` table records each ticket purchase, linking users to trains and capturing the purchase time.
      ```sql
      CREATE TABLE purchases (
          user_email VARCHAR(255),
          train_id VARCHAR(255),
          purchase_time DATETIME,
          FOREIGN KEY (user_email) REFERENCES users(email),
          FOREIGN KEY (train_id) REFERENCES trains(id)
      );
      ```

- **NoSQL (Neo4j)**:
  - **Usage**: 
    - Handling the complex relationships between train stations and routes.
    - Efficiently managing and querying interconnected data such as finding the shortest path between stations.
  - **Examples**:
    - **Stations and Connections**: Stations are nodes and connections between stations are relationships. This structure allows for efficient traversal and querying.
      ```cypher
      CREATE (s1:Station {id: 'station1', details: 'Details of station 1'});
      CREATE (s2:Station {id: 'station2', details: 'Details of station 2'});
      CREATE (s1)-[:CONNECTED_TO {travel_time: 10}]->(s2);
      ```
    - **Route Queries**: Finding all routes from one station to another can be efficiently performed using graph traversal queries.
      ```cypher
      MATCH (start:Station {id: 'station1'})-[:CONNECTED_TO*]->(end:Station {id: 'station2'})
      RETURN start, end;
      ```

### Schema

  - **`users`**: Stores user emails and additional details.
    - `email` (VARCHAR): Primary key.
    - `details` (TEXT): Additional user information.
  - **`trains`**: Stores details of each train, including capacity and status.
    - `id` (VARCHAR): Primary key.
    - `capacity` (INT): Train capacity.
    - `status` (VARCHAR): Operational status (e.g., OPERATIONAL, DELAYED).
  - **`stations`**: Stores details about train stations.
    - `id` (VARCHAR): Primary key.
    - `details` (TEXT): Information about the station.
  - **`purchases`**: Logs ticket purchases by users.
    - `user_email` (VARCHAR): Foreign key to `users`.
    - `train_id` (VARCHAR): Foreign key to `trains`.
    - `purchase_time` (DATETIME): Time of the ticket purchase.

### Designing the Solution

#### Requirements

1. **Entities**:
    - User
    - Train
    - Station
    - Schedule
    - Purchase

2. **Operations**:
    - Add, retrieve, and delete users.
    - Add, update, retrieve, and delete trains.
    - Add and retrieve stations.
    - Connect stations.
    - Add and retrieve schedules.
    - Buy tickets and retrieve purchase history.

#### Relational Model

1. **User**:
   - **Attributes**: `email`, `details`
   - **Operations**:
     - Add a new user.
     - Retrieve all users.
     - Delete a user.
   - **SQL Schema**:
     ```sql
     CREATE TABLE users (
         email VARCHAR(255) PRIMARY KEY,
         details TEXT
     );
     ```
   - **SQL Queries**:
     - Add User:
       ```sql
       INSERT INTO users (email, details) VALUES (?, ?);
       ```
     - Retrieve All Users:
       ```sql
       SELECT email FROM users;
       ```
     - Delete User:
       ```sql
       DELETE FROM users WHERE email = ?;
       ```

2. **Train**:
   - **Attributes**: `id`, `capacity`, `status`
   - **Operations**:
     - Add a new train.
     - Update train details.
     - Retrieve train status.
     - Delete a train.
   - **SQL Schema**:
     ```sql
     CREATE TABLE trains (
         id VARCHAR(255) PRIMARY KEY,
         capacity INT,
         status VARCHAR(255)
     );
     ```
   - **SQL Queries**:
     - Add Train:
       ```sql
       INSERT INTO trains (id, capacity, status) VALUES (?, ?, ?);
       ```
     - Update Train Details:
       ```sql
       UPDATE trains SET capacity = ?, status = ? WHERE id = ?;
       ```
     - Retrieve Train Status:
       ```sql
       SELECT status FROM trains WHERE id = ?;
       ```
     - Delete Train:
       ```sql
       DELETE FROM trains WHERE id = ?;
       ```

3. **Station**:
   - **Attributes**: `id`, `details`
   - **Operations**:
     - Add a new station.
     - Connect stations.
   - **SQL Schema**:
     ```sql
     CREATE TABLE stations (
         id VARCHAR(255) PRIMARY KEY,
         details TEXT
     );
     ```
   - **SQL Queries**:
     - Add Station:
       ```sql
       INSERT INTO stations (id, details) VALUES (?, ?);
       ```
     - Connect Stations (NoSQL - Neo4j):
       ```cypher
       MATCH (start:Station {id: $start_id}), (end:Station {id: $end_id})
       CREATE (start)-[:CONNECTED_TO {travel_time: $travel_time}]->(end);
       ```

4. **Schedule**:
   - **Attributes**: `train_id`, `start_time`, `valid_from`, `valid_until`
   - **Operations**:
     - Add a schedule.
     - Retrieve all schedules.
   - **NoSQL Schema**:
     ```cypher
     CREATE (s:Schedule {
         id: $schedule_id,
         train_id: $train_id,
         start_time: $start_time,
         valid_from: $valid_from,
         valid_until: $valid_until
     });
     ```
   - **NoSQL Queries**:
     - Add Schedule:
       ```cypher
       CREATE (s:Schedule {
           id: $schedule_id,
           train_id: $train_id,
           start_time: $start_time,
           valid_from: $valid_from,
           valid_until: $valid_until
       });
       ```

5. **Purchase**:
   - **Attributes**: `user_email`, `train_id`, `purchase_time`
   - **Operations**:
     - Buy a ticket.
     - Retrieve purchase history.
   - **SQL Schema**:
     ```sql
     CREATE TABLE purchases (
         user_email VARCHAR(255),
         train_id VARCHAR(255),
         purchase_time DATETIME,
         FOREIGN KEY (user_email) REFERENCES users(email),
         FOREIGN KEY (train_id) REFERENCES trains(id)
     );
     ```
   - **SQL Queries**:
     - Buy Ticket:
       ```sql
       INSERT INTO purchases (user_email, train_id, purchase_time) VALUES (?, ?, ?);
       ```
     - Retrieve Purchase History:
       ```sql
       SELECT * FROM purchases WHERE user_email = ? ORDER BY purchase_time DESC;
       ```

### Queries and Their Purposes

1. **Adding Users**:
   - Purpose: To register a new user in the system.
   - Query:
     ```sql
     INSERT INTO users (email, details) VALUES (?, ?);
     ```

2. **Adding Trains**:
   - Purpose: To register a new train with specific capacity and status.
   - Query:
     ```sql
     INSERT INTO trains (id, capacity, status) VALUES (?, ?, ?);
     ```

3. **Connecting Stations**:
   - Purpose: To establish a route between two stations.
   - Query (Neo4j):
     ```cypher
     MATCH (start:Station {id: $start_id}), (end:Station {id: $end_id})
     CREATE (start)-[:CONNECTED_TO {travel_time: $travel_time}]->(end);
     ```

4. **Adding Schedules**:
   - Purpose: To define a schedule for a train, specifying stops and validity.
   - Query (Neo4j):
     ```cypher
     CREATE (s:Schedule {
         id: $schedule_id,
         train_id: $train_id,
         start_time: $start_time,
         valid_from: $valid_from,
         valid_until: $valid_until
     });
     ```

5. **Buying Tickets**:
   - Purpose: To allow users to purchase tickets for specific train schedules.
   - Query:
     ```sql
     INSERT INTO purchases (user_email, train_id, purchase_time) VALUES (?, ?, ?);
     ```

### Motivation for Using NoSQL/SQL

- **SQL**:
  - Used for structured data with clear relationships (e.g., Users, Trains, Purchases).
  - Relational databases provide strong consistency and are suitable for transactions like ticket purchases.

- **NoSQL (Neo4j)**:
  - Used for graph-related data (e.g., Stations and their connections, Schedules).
  - Graph databases are optimal for traversing relationships and querying connected data efficiently.

#### User Tests

1. **test_add_and_fetch_user**
   - **Purpose**: Validate adding a user and fetching all users.
   - **Validation**: 
     - Adds a user to the system.
     - Fetches all users and verifies the new user is included.

2. **test_add_and_delete_user**
   - **Purpose**: Validate adding and deleting a user.
   - **Validation**: 
     - Adds a user to the system.
     - Deletes the user.
     - Fetches all users and verifies the user is no longer present.

3. **test_buy_ticket_non_existing_user**
   - **Purpose**: Ensure the system prevents ticket purchases for non-existing users.
   - **Validation**: 
     - Attempts to buy a ticket for a non-existing user, raises `ValueError`.

4. **test_get_purchase_history_non_existing_user**
   - **Purpose**: Ensure the system returns an empty purchase history for non-existing users.
   - **Validation**: 
     - Fetches the purchase history for a non-existing user and verifies it is empty.

5. **test_add_user_invalid_email**
   - **Purpose**: Ensure the system raises an error for invalid email addresses.
   - **Validation**: 
     - Attempts to add a user with an invalid email address, raises `ValueError`.

6. **test_add_user_with_long_email**
   - **Purpose**: Validate adding a user with an exceptionally long email address.
   - **Validation**: 
     - Adds a user with a long email address.
     - Verifies the user is added correctly.

7. **test_empty_purchase_history_for_new_user**
   - **Purpose**: Ensure new users have an empty purchase history.
   - **Validation**: 
     - Adds a new user.
     - Verifies the user's purchase history is empty.

#### Train Tests

1. **test_add_and_fetch_train**
   - **Purpose**: Validate adding a train and fetching its status.
   - **Validation**: 
     - Adds a train to the system.
     - Fetches the train's status and verifies it matches the added status.

2. **test_add_train_with_invalid_data**
   - **Purpose**: Ensure the system raises an error for adding duplicate trains.
   - **Validation**: 
     - Adds a train to the system.
     - Attempts to add the same train again, raises `ValueError`.

3. **test_delete_train_and_verify_cascading_deletes**
   - **Purpose**: Validate deleting a train and ensuring related data is removed.
   - **Validation**: 
     - Adds a train.
     - Deletes the train.
     - Verifies the train is removed from both RDBMS and Neo4j.

4. **test_update_train_details_partial**
   - **Purpose**: Validate partial updates to train details.
   - **Validation**: 
     - Adds a train.
     - Partially updates the train's capacity.
     - Verifies the capacity is updated while the status remains unchanged.

5. **test_update_train_status_delayed_and_broken**
   - **Purpose**: Validate updating train status to `DELAYED` and `BROKEN`.
   - **Validation**: 
     - Adds a train.
     - Updates the train's status to `DELAYED` and then `BROKEN`.
     - Verifies the status updates correctly.

6. **test_add_train_with_invalid_status**
   - **Purpose**: Ensure the system raises an error for invalid train statuses.
   - **Validation**: 
     - Attempts to add a train with an invalid status, raises `AttributeError`.

7. **test_update_train_capacity_to_invalid_value**
   - **Purpose**: Ensure the system raises an error for invalid train capacity updates.
   - **Validation**: 
     - Adds a train.
     - Attempts to update the train capacity to an invalid value, raises `ValueError`.

8. **test_add_and_retrieve_trains**
   - **Purpose**: Validate adding and retrieving trains.
   - **Validation**: 
     - Adds a train.
     - Retrieves the train and verifies its details.
     - Deletes the train for cleanup.

9. **test_update_train_capacity_and_status**
   - **Purpose**: Validate updating both capacity and status of a train.
   - **Validation**: 
     - Adds a train.
     - Updates the train's capacity and status.
     - Verifies the updates are correctly reflected.

10. **test_add_train_with_zero_capacity**
    - **Purpose**: Ensure the system raises an error for adding a train with zero capacity.
    - **Validation**: 
      - Attempts to add a train with zero capacity, raises `ValueError`.

#### Station Tests

1. **test_add_and_connect_train_stations**
   - **Purpose**: Validate adding and connecting train stations.
   - **Validation**: 
     - Adds two train stations.
     - Connects the stations.
     - Verifies the connection in Neo4j.

2. **test_connect_train_stations_invalid_travel_time**
   - **Purpose**: Ensure the system raises an error for invalid travel times.
   - **Validation**: 
     - Adds two train stations.
     - Attempts to connect them with an invalid travel time, raises `ValueError`.

3. **test_connect_stations_already_connected**
   - **Purpose**: Ensure the system raises an error for connecting already connected stations.
   - **Validation**: 
     - Adds and connects two train stations.
     - Attempts to connect them again, raises `ValueError`.

4. **test_connect_nonexistent_train_stations**
   - **Purpose**: Ensure the system raises an error for connecting non-existent stations.
   - **Validation**: 
     - Attempts to connect non-existent stations, raises `ValueError`.

5. **test_add_train_station_with_duplicate_key**
   - **Purpose**: Ensure the system raises an error for adding duplicate train stations.
   - **Validation**: 
     - Adds a train station.
     - Attempts to add the same station again, raises `ValueError`.

6. **test_connect_stations_with_zero_travel_time**
   - **Purpose**: Ensure the system raises an error for connecting stations with zero travel time.
   - **Validation**: 
     - Adds two train stations.
     - Attempts to connect them with zero travel time, raises `ValueError`.

7. **test_add_and_connect_multiple_stations**
   - **Purpose**: Validate adding and connecting multiple train stations.
   - **Validation**: 
     - Adds multiple train stations.
     - Connects the stations sequentially.
     - Verifies each connection.

#### Schedule Tests

1. **test_add_schedule_with_one_stop**
   - **Purpose**: Ensure the system raises an error for schedules with only one stop.
   - **Validation**: 
     - Adds a train and one station.
     - Attempts to add a schedule with only one stop, raises `ValueError`.

2. **test_add_schedule_with_unconnected_stops**
   - **Purpose**: Ensure the system raises an error for schedules with unconnected stops.
   - **Validation**: 
     - Adds three stations and connects only two.
     - Attempts to add a schedule including the unconnected stop, raises `ValueError`.

3. **test_add_schedule_with_invalid_stops**
   - **Purpose**: Ensure the system raises an error for invalid stops in a schedule.
   - **Validation**: 
     - Adds a train and two stations without connecting them.
     - Attempts to add a schedule with these stops, raises `ValueError`.

4. **test_add_schedule_with_overlapping_validity_periods**
   - **Purpose**: Ensure the system raises an error for overlapping schedule validity periods.
   - **Validation**: 
     - Adds a train and two schedules with overlapping periods.
     - Attempts to add the overlapping schedule, raises `ValueError`.

5. **test_add_and_retrieve_multiple_schedules**
   - **Purpose**: Validate adding and retrieving multiple schedules.
   - **Validation**: 
     - Adds a train and multiple schedules.
     - Verifies both schedules are retrievable.

#### Ticket Tests

1. **test_buy_ticket_and_reserve_seats**
   - **Purpose**: Validate ticket purchase with seat reservation.
   - **Validation**: 
     - Adds a user, train, stations, and schedule.
     - Buys a ticket with seat reservation.
     - Verifies the purchase in the user's history.

2. **test_buy_ticket_without_seat_reservation**
   - **Purpose**: Validate ticket purchase without seat reservation.
   - **Validation**: 
     - Adds a user, train, stations, and schedule.
     - Buys a ticket without seat reservation.
     - Verifies the purchase in the user's history.

3. **test_purchase_tickets_for_multiple_trains**
   - **Purpose**: Validate ticket purchases for multiple trains.
   - **Validation**: 
     - Adds a user, trains, stations, and schedules.
     - Buys tickets for multiple trains.
     - Verifies both purchases in the user's history.

4. **test_user_purchase_history**
   - **Purpose**: Validate retrieval of user's purchase history.
   - **Validation**: 
     - Adds a user, train, stations, and schedule.
     - Buys a ticket.
     - Verifies the purchase in the user's history.

#### Connection Tests

1. **test_search_connections**
   - **Purpose**: Validate searching for connections between stations.
   - **Validation**: 
     - Adds stations and connects them.
     - Searches for connections and verifies they are found.

2. **test_search_connections_no_stations**
   - **Purpose**: Ensure the system raises an error for searching connections with non-existing stations.
   - **Validation**: 
     - Attempts to search connections between non-existing stations, raises `ValueError`.

3. **test_search_connections_with_existing_stations**
   - **Purpose**: Validate searching for connections between existing stations.
   - **Validation**: 
     - Adds stations and connects them.
     - Searches for connections and verifies they are found.

4. **test_search_connections_with_multiple_criteria**
   - **Purpose**: Validate searching for connections with multiple sorting criteria.
   - **Validation**: 
     - Adds stations, trains, and schedules.
     - Searches for connections sorted by various criteria.
     - Verifies the connections are found.