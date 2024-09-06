import pytest
from pytest_mysql import factories
from contextlib import contextmanager
from neo4j import GraphDatabase
from pytest_mysql.executor_noop import NoopMySQLExecutor
from public.traits.interface import BASE_USER_NAME, BASE_USER_PASS, ADMIN_USER_NAME, ADMIN_USER_PASS
from traits.implementation import TraitsUtility

################################################################################
# MariaDB fixtures
################################################################################

@pytest.fixture(scope="session")
def mariadb_host(request):
    return request.config.getoption("--mysql-host") if request.config.getoption("--mysql-host") is not None else "127.0.0.1"

@pytest.fixture(scope="session")
def mariadb_port(request):
    return request.config.getoption("--mysql-port") if request.config.getoption("--mysql-port") is not None else 3306

@pytest.fixture(scope="session")
def root_mariadb_in_docker(mariadb_host, mariadb_port):
    mysql_executor = NoopMySQLExecutor(
        user="root",
        host=mariadb_host,
        port=int(mariadb_port),
    )

    with mysql_executor:
        yield mysql_executor

root_connection = factories.mysql("root_mariadb_in_docker", passwd="root-pass")

@pytest.fixture
def mariadb_database():
    return "test"

@pytest.fixture
def mariadb(root_connection):
    cur = root_connection.cursor()
    cur.execute("BEGIN;")
    for sql_statement in TraitsUtility.generate_sql_initialization_code():
        cur.execute(sql_statement)
    cur.execute("COMMIT;")
    yield root_connection

@pytest.fixture
def connection_factory(mariadb, mariadb_host, mariadb_port, mariadb_database):
    @contextmanager
    def _gen_connection(user, password):
        import mysql.connector
        from mysql.connector import Error

        assert user != "root", "Do not create connections to the db using Root!"
        connection = mysql.connector.connect(host=mariadb_host,
                                             database=mariadb_database,
                                             user=user,
                                             port=mariadb_port,
                                             password=password)
        try:
            if connection.is_connected():
                db_Info = connection.get_server_info()
                cursor = connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                yield connection
        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    yield _gen_connection

@pytest.fixture
def rdbms_connection(connection_factory):
    with connection_factory(BASE_USER_NAME, BASE_USER_PASS) as connection:
        yield connection

@pytest.fixture
def rdbms_admin_connection(connection_factory):
    with connection_factory(ADMIN_USER_NAME, ADMIN_USER_PASS) as connection:
        yield connection

################################################################################
# Neo4J fixtures
################################################################################

def pytest_addoption(parser):
    try:
        parser.addoption(
            '--neo4j-web-port', action='store', default="", help='Web Port to connect to Neo4j'
        )
        parser.addoption(
            '--neo4j-bolt-port', action='store', default="", help='Bolt Port to connect to Neo4j'
        )
        parser.addoption(
            '--neo4j-host', action='store', default="localhost", help='Bolt Port to connect to Neo4j'
        )
    except Exception:
        pass

@pytest.fixture
def neo4j_db_port(request):
    return request.config.getoption("--neo4j-bolt-port")

@pytest.fixture
def neo4j_db_host(request):
    return request.config.getoption("--neo4j-host")

@pytest.fixture
def neo4j_db(neo4j_db_host, neo4j_db_port):
    URI = f"neo4j://{neo4j_db_host}:{neo4j_db_port}"
    with GraphDatabase.driver(URI) as driver:
        driver.verify_connectivity()
        records, summary, keys = driver.execute_query("MATCH (a) DETACH DELETE a")
        yield driver
        records, summary, keys = driver.execute_query("MATCH (a) DETACH DELETE a")
