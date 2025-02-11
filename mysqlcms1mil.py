import pandas as pd
import pymysql
import time
import numpy as np
import os

# MySQL connection
MYSQL_HOST = '172.18.0.2'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'dbpasscms'
MYSQL_DATABASE = 'course_management_system'

dataset = '1_mil_records.csv'
NUM_EXPERIMENTS = 31

# Function to connect to MySQL database
def connect_to_db():
    connection = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    return connection

# Function to create the database
def create_database(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
    print(f"Database '{MYSQL_DATABASE}' created or already exists.")

# Function to create tables
def create_tables(cursor):
    tables = {
        "Courses": (
            "CREATE TABLE IF NOT EXISTS Courses ("
            "  course_id INT PRIMARY KEY,"
            "  course_name VARCHAR(255),"
            "  course_content TEXT"
            ")"
        ),
        "Students": (
            "CREATE TABLE IF NOT EXISTS Students ("
            "  student_id INT PRIMARY KEY,"
            "  student_name VARCHAR(255),"
            "  student_email_address VARCHAR(255),"
            "  course_id INT,"
            "  FOREIGN KEY (course_id) REFERENCES Courses(course_id)"
            ")"
        ),
        "Professors": (
            "CREATE TABLE IF NOT EXISTS Professors ("
            "  professor_id INT PRIMARY KEY,"
            "  professor_name VARCHAR(255),"
            "  professor_email_address VARCHAR(255),"
            "  course_id INT,"
            "  FOREIGN KEY (course_id) REFERENCES Courses(course_id)"
            ")"
        ),
        "Assignments": (
            "CREATE TABLE IF NOT EXISTS Assignments ("
            "  assignment_id INT PRIMARY KEY,"
            "  assignment_title VARCHAR(255),"
            "  submission_status VARCHAR(3),"
            "  score INT,"
            "  student_id INT,"
            "  course_id INT,"
            "  FOREIGN KEY (student_id) REFERENCES Students(student_id),"
            "  FOREIGN KEY (course_id) REFERENCES Courses(course_id)"
            ")"
        )
    }

    for table_name, create_query in tables.items():
        cursor.execute(create_query)
        print(f"Created or verified table: {table_name}")

# Function to insert data into each tables
def insert_data(cursor, df):
    # Inserting data into Courses table
    courses_data = [
        (row["course_id"], row["course_name"], row["course_content"])
        for _, row in df.iterrows()
    ]
    cursor.executemany(
        "INSERT IGNORE INTO Courses (course_id, course_name, course_content) "
        "VALUES (%s, %s, %s)",
        courses_data
    )

    # Inserting data into Students table
    students_data = [
        (row["student_id"], row["student_name"], row["student_email_address"], row["course_id"])
        for _, row in df.iterrows()
    ]
    cursor.executemany(
        "INSERT IGNORE INTO Students (student_id, student_name, student_email_address, course_id) "
        "VALUES (%s, %s, %s, %s)",
        students_data
    )

    # Inserting data into Professors table
    professors_data = [
        (row["professor_id"], row["professor_name"], row["professor_email_address"], row["course_id"])
        for _, row in df.iterrows()
    ]
    cursor.executemany(
        "INSERT IGNORE INTO Professors (professor_id, professor_name, professor_email_address, course_id) "
        "VALUES (%s, %s, %s, %s)",
        professors_data
    )

    # Inserting data into Assignments table
    assignments_data = [
        (row["assignment_id"], row["assignment_title"], row["submission_status"], row["score"], row["student_id"], row["course_id"])
        for _, row in df.iterrows()
    ]
    cursor.executemany(
        "INSERT IGNORE INTO Assignments (assignment_id, assignment_title, submission_status, score, student_id, course_id) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        assignments_data
    )

# Function to run the query and measure the execution times
def run_query(cursor, query):
    start_time = time.time()
    cursor.execute(query)
    end_time = time.time()
    return (end_time - start_time) * 1000  # returning time in milliseconds

# Query functions for MySQL
def query_1(cursor):
    query = """
        SELECT student_id, student_name 
        FROM Students 
        WHERE course_id = (SELECT course_id FROM Courses WHERE course_name = 'Data Analysis' LIMIT 1) 
        LIMIT 10;
    """
    return run_query(cursor, query)

def query_2(cursor):
    query = """
        SELECT student_id, student_name
        FROM Students
        WHERE course_id IN (SELECT course_id FROM Courses WHERE course_name = 'Data Analysis')
        AND student_id IN (SELECT student_id FROM Assignments WHERE submission_status = 'yes')
        LIMIT 10;
    """
    return run_query(cursor, query)

def query_3(cursor):
    # Updating
    update_query = """
        UPDATE Assignments 
        SET submission_status = 'Yes', score = 30 
        WHERE student_id IN (540214, 533994);
    """
    run_query(cursor, update_query)

    # Fetching
    select_query = """
        SELECT 
            s.student_id, 
            s.student_name, 
            c.course_id, 
            c.course_name, 
            a.assignment_id, 
            a.submission_status, 
            a.score
        FROM Students s
        JOIN Courses c ON s.course_id = c.course_id
        JOIN Assignments a ON s.student_id = a.student_id
        WHERE s.student_id IN (540214, 533994);
    """
    return run_query(cursor, select_query)

def query_4(cursor):
    query = """
        SELECT s.student_id, s.student_name, s.course_id, c.course_name, a.submission_status, a.score
        FROM Students s
        JOIN Courses c ON s.course_id = c.course_id
        JOIN Assignments a ON s.student_id = a.student_id
        WHERE c.course_name = 'Data Analysis' 
        AND a.submission_status = 'yes'
        AND a.score > 26;
    """
    return run_query(cursor, query)

# Function to get first and average execution times
def run_experiments(cursor, query_func, num_experiments=NUM_EXPERIMENTS):
    first_execution_time = query_func(cursor)
    execution_times = [first_execution_time]  # Storing the first execution time separately

    # Running the query 30 more times to calculate it's average
    for _ in range(num_experiments - 1):
        execution_time = query_func(cursor)
        execution_times.append(execution_time)

    avg_execution_time = np.mean(execution_times[1:])  # Average of the 30 execution times
    return execution_times, first_execution_time, avg_execution_time

# Main function
def main():
    # Loading the dataset
    data = pd.read_csv(dataset)

    # Connect to MySQL and getting the cursor
    connection = connect_to_db()
    cursor = connection.cursor()

    # Creating output directory
    output_dir = "/app/output"
    os.makedirs(output_dir, exist_ok=True)

    # DataFrame to store experiment results
    results = []

    # Running for different data sizes
    for i, size in enumerate([250000, 500000, 750000, 1000000]):
        print(f"Running experiments for {size} records...")
        df_subset = data.iloc[:size]

        # Creating database and tables
        create_database(cursor)
        cursor.execute(f"USE {MYSQL_DATABASE}")
        create_tables(cursor)

        # Inserting data
        insert_data(cursor, df_subset)
        connection.commit()

        # Running experiments for each query
        for query_name, query_func in {
            "query_1": query_1,
            "query_2": query_2,
            "query_3": query_3,
            "query_4": query_4
        }.items():
            print(f"Running {query_name} for {size} records...")
            execution_times, first_execution_time, avg_execution_time = run_experiments(cursor, query_func)

            # Adding results to the DataFrame
            results.append({
                "Records": size,
                "Query": query_name,
                "First Execution Time (ms)": first_execution_time,
                "Average Execution Time (ms)": avg_execution_time
            })

        # Dropping the database
        cursor.execute(f"DROP DATABASE {MYSQL_DATABASE}")
        print(f"Database '{MYSQL_DATABASE}' dropped.")

    # Saving results to an Excel file
    output_file = os.path.join(output_dir, "mysql_query_execution_times.xlsx")
    results_df = pd.DataFrame(results)
    results_df.to_excel(output_file, index=False)
    print(f"Results saved to {output_file}")

    # Closing the MySQL connection
    cursor.close()
    connection.close()
    print("Experiments completed successfully.")

if __name__ == "__main__":
    main()