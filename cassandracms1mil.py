import os
import time
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

dataset = "1_mil_records.csv"
save_dir = "/app/output"
os.makedirs(save_dir, exist_ok=True)

# Function to connect to Cassandra
def connect_to_cassandra():
    cluster = Cluster(["172.18.0.4"], port=9042)
    session = cluster.connect()
    print("Connected to Cassandra")
    return session

# Function to create keyspace
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS course_management_system
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    print("Keyspace 'course_management_system' is created or already exists.")
    session.set_keyspace("course_management_system")

# Function to create tables
def create_tables(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS Courses (
            course_id INT PRIMARY KEY,
            course_name TEXT,
            course_content TEXT
        );
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS Students (
            student_id INT PRIMARY KEY,
            student_name TEXT,
            student_email_address TEXT,
            course_id INT
        );
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS Assignments (
            assignment_id INT PRIMARY KEY,
            assignment_title TEXT,
            submission_status TEXT,
            score INT,
            student_id INT,
            course_id INT
        );
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS Professors (
            professor_id INT PRIMARY KEY,
            professor_name TEXT,
            professor_email_address TEXT,
            course_id INT
        );
    """)
    print("Tables created successfully.")

# Function to load dataset and insert data using batch processing
def insert_data_from_csv(session, dataset, num_records):
    df = pd.read_csv(dataset, nrows=num_records)
    
    # Prepare statements
    insert_courses = session.prepare("""
        INSERT INTO Courses (course_id, course_name, course_content) VALUES (?, ?, ?);
    """)
    insert_students = session.prepare("""
        INSERT INTO Students (student_id, student_name, student_email_address, course_id) VALUES (?, ?, ?, ?);
    """)
    insert_assignments = session.prepare("""
        INSERT INTO Assignments (assignment_id, assignment_title, submission_status, score, student_id, course_id) VALUES (?, ?, ?, ?, ?, ?);
    """)
    insert_professors = session.prepare("""
        INSERT INTO Professors (professor_id, professor_name, professor_email_address, course_id) VALUES (?, ?, ?, ?);
    """)

    # Batch insert data in chunks to avoid exceeding the batch statement limit
    batch_size = 100
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    # Function to execute the batch after adding a chunk of records
    def execute_batch():
        session.execute(batch)
        batch.clear()
    
    for i, row in df.iterrows():
        batch.add(insert_courses, (row['course_id'], row['course_name'], row['course_content']))
        batch.add(insert_students, (row['student_id'], row['student_name'], row['student_email_address'], row['course_id']))
        batch.add(insert_assignments, (row['assignment_id'], row['assignment_title'], row['submission_status'], row['score'], row['student_id'], row['course_id']))
        batch.add(insert_professors, (row['professor_id'], row['professor_name'], row['professor_email_address'], row['course_id']))
        
        # If batch size exceeds the limit, execute it and start a new one
        if len(batch) >= batch_size:
            execute_batch()

    # Execute any remaining records in the batch
    if len(batch) > 0:
        execute_batch()

    print(f"Data inserted from first {num_records} records...")

# Query functions for Cassandra
def query_1(session):
    query = "SELECT student_id, student_name FROM Students WHERE course_id = 100015 LIMIT 10 ALLOW FILTERING;"
    return session.execute(query)

def query_2(session):
    # Step 1: Getting student IDs from Assignments
    select_query = "SELECT student_id FROM Assignments WHERE course_id = 100015 AND submission_status = 'Yes' LIMIT 10 ALLOW FILTERING;"
    student_ids = [row.student_id for row in session.execute(select_query)]
    
    # Step 2: Getting student details from Students
    if student_ids:
        retrieve_query = f"SELECT student_id, student_name FROM Students WHERE course_id = 100015 AND student_id IN ({', '.join(map(str, student_ids))}) ALLOW FILTERING;"
        return session.execute(retrieve_query)
    return []

def query_3(session):
    # Step 1: Updating Assignments
    update_query = "UPDATE Assignments SET submission_status = 'Yes', score = 30 WHERE assignment_id IN (167615, 149316);"
    session.execute(update_query)
    
    # Step 2: Fetching updated student and course data
    select_students_query = "SELECT student_id, student_name FROM Students WHERE student_id IN (540214, 533994) ALLOW FILTERING;"
    select_courses_query = "SELECT course_id, course_name FROM Courses WHERE course_id = 100015 ALLOW FILTERING;"
    
    students = session.execute(select_students_query)
    courses = session.execute(select_courses_query)
    
    return list(students), list(courses)

def query_4(session):
    # Step 1: Getting student IDs from Assignments
    select_query = "SELECT student_id FROM Assignments WHERE course_id = 100015 AND submission_status = 'Yes' AND score > 26 ALLOW FILTERING;"
    student_ids = [row.student_id for row in session.execute(select_query)]
    
    # Step 2: Getting student details from Students
    if student_ids:
        retrieve_query = f"SELECT student_id, student_name FROM Students WHERE course_id = 100015 AND student_id IN ({', '.join(map(str, student_ids))}) ALLOW FILTERING;"
        students = session.execute(retrieve_query)
        
        # Step 3: Getting course details
        courses_query = "SELECT course_id, course_name FROM Courses WHERE course_id = 100015 ALLOW FILTERING;"
        courses = session.execute(courses_query)
        
        return list(students), list(courses)
    return [], []

# Function to run the query and measure execution times
def run_query(session, query_func):
    start_time = time.time()
    result = query_func(session)
    end_time = time.time()
    return (end_time - start_time) * 1000  # return time in milliseconds

# Function to get first and average execution times
def run_experiments(session, query_func, num_experiments=31):
    first_execution_time = run_query(session, query_func)
    execution_times = [first_execution_time]  # Store the first execution time separately

    # Run the query 30 more times to calculate it's average
    for _ in range(num_experiments - 1):
        execution_time = run_query(session, query_func)
        execution_times.append(execution_time)

    avg_execution_time = np.mean(execution_times[1:])  # Average of the 30 execution times
    return execution_times, first_execution_time, avg_execution_time

# Main function
def main():
    # Connecting to Cassandra
    session = connect_to_cassandra()

    # DataFrame to store experiment results
    results = []

    # Running for different data sizes
    for num_records in [250000, 500000, 750000, 1000000]:
        print(f"Running experiments for {num_records} records...")

        # Dropping the keyspace and tables
        session.execute("DROP KEYSPACE IF EXISTS course_management_system;")
        create_keyspace(session)
        create_tables(session)

        # Inserting data
        insert_data_from_csv(session, dataset, num_records)

        # Running experiments for each query
        for query_name, query_func in {
            "query_1": query_1,
            "query_2": query_2,
            "query_3": query_3,
            "query_4": query_4
        }.items():
            print(f"Running {query_name} for {num_records} records...")
            execution_times, first_execution_time, avg_execution_time = run_experiments(session, query_func)

            # Adding results to the DataFrame
            results.append({
                "Records": num_records,
                "Query": query_name,
                "First Execution Time (ms)": first_execution_time,
                "Average Execution Time (ms)": avg_execution_time
            })

        print(f"Experiment for {num_records} records completed.")

    # Saving results to an Excel file
    file_path = os.path.join(save_dir, "cassandra_query_execution_times.xlsx")
    results_df = pd.DataFrame(results)
    results_df.to_excel(file_path, index=False)
    print(f"Results saved to '{file_path}'")

if __name__ == "__main__":
    main()