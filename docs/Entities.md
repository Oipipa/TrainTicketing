## Relational Model

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
