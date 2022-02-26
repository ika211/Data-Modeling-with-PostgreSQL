from sql_queries import create_tables_list, drop_tables_list
import psycopg2


def create_database():
    """
    Create a Sparkify database and return a cursor and a connection to
    the database

    Returns:
         cursor object, connection object
    """

    # connecting to default database
    connection = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=password")
    connection.set_session(autocommit=True)
    cursor = connection.cursor()

    # Creating our sparkify Database
    cursor.execute("DROP DATABASE IF EXISTS sparkify ")
    cursor.execute("CREATE DATABASE sparkify")

    # closing the connection to default database
    connection.close()

    # connecting to the sparkify database
    connection = psycopg2.connect("host=127.0.0.1 dbname=sparkify user=postgres password=password")
    cursor = connection.cursor()

    return cursor, connection


def drop_tables(cursor, connection):
    """
    Drop the tables if they are already present.

    Args:
        cursor: cursor object for db
        connection: connection object for db

    Return:
        None
    """

    for drop_query in drop_tables_list:
        cursor.execute(drop_query)
        connection.commit()


def create_tables(cursor, connection):
    """
        Create the tables in Sparkify Database.

        Args:
            cursor: cursor object for db
            connection: connection object for db

        Return:
            None
        """

    for create_query in create_tables_list:
        cursor.execute(create_query)
        connection.commit()


def main():
    """
    main method for the script.

    Return:
         None
    """

    cursor, connection = create_database()

    drop_tables(cursor, connection)
    create_tables(cursor, connection)

    print("Tables are created!!!")
    connection.close()


if __name__ == "__main__":
    main()
