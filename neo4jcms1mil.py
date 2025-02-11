from neo4j import GraphDatabase
import pandas as pd
import time
import os
import numpy as np

# Neo4j Connection
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "dbpasscms"

DATASET = '1_mil_records.csv'
NUM_EXPERIMENTS = 31

class Neo4jCMS:
    # Function to connect to Neo4j database
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # Function to create the database
    def clear_database(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared.")

    # Function to insert data into each tables
    def insert_batch_data(self, df, batch_size=10000):
        with self.driver.session() as session:
            try:
                # Inserting data in smaller batches to prevent memory issues
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    # Inserting courses in batches
                    session.run("""
                    UNWIND $courses AS course
                    CREATE (c:Course {course_id: course.course_id, course_name: course.course_name, course_content: course.course_content})
                    """, courses=batch[['course_id', 'course_name', 'course_content']].to_dict(orient='records'))

                    # Inserting students in batches
                    session.run("""
                    UNWIND $students AS student
                    CREATE (s:Student {student_id: student.student_id, student_name: student.student_name, student_email_address: student.student_email_address})
                    WITH s, student
                    MATCH (c:Course {course_id: student.course_id})
                    CREATE (s)-[:ENROLLED_IN]->(c)
                    """, students=batch[['student_id', 'student_name', 'student_email_address', 'course_id']].to_dict(orient='records'))

                    # Inserting professors in batches
                    session.run("""
                    UNWIND $professors AS professor
                    CREATE (p:Professor {professor_id: professor.professor_id, professor_name: professor.professor_name, professor_email_address: professor.professor_email_address})
                    WITH p, professor
                    MATCH (c:Course {course_id: professor.course_id})
                    CREATE (p)-[:TEACHES]->(c)
                    """, professors=batch[['professor_id', 'professor_name', 'professor_email_address', 'course_id']].to_dict(orient='records'))

                    # Inserting assignments in batches
                    session.run("""
                    UNWIND $assignments AS assignment
                    CREATE (a:Assignment {assignment_id: assignment.assignment_id, assignment_title: assignment.assignment_title, submission_status: assignment.submission_status, score: assignment.score})
                    WITH a, assignment
                    MATCH (s:Student {student_id: assignment.student_id})
                    CREATE (s)-[:SUBMITTED]->(a)
                    """, assignments=batch[['assignment_id', 'assignment_title', 'submission_status', 'score', 'student_id', 'course_id']].to_dict(orient='records'))
                    
            except Exception as e:
                print(f"Error during batch insertion: {e}")
            else:
                print(f"Batch data inserted for {len(df)} records.")

    # Function to run the query and measure the execution times
    def run_query(self, query, params=None):
        with self.driver.session() as session:
            start_time = time.time()
            session.run(query, params or {})
            end_time = time.time()
        return (end_time - start_time) * 1000  # Return execution time in milliseconds

    # Function to get first and average execution times
    def run_experiments(self, query, params=None):
        first_execution_time = self.run_query(query, params)
        execution_times = [first_execution_time] # Storing the first execution time separately
        
        # Running the query 30 more times to calculate it's average
        for _ in range(NUM_EXPERIMENTS - 1):
            execution_times.append(self.run_query(query, params))
            
        avg_execution_time = np.mean(execution_times[1:]) # Average of the 30 execution times
        return execution_times, first_execution_time, avg_execution_time

def main():
    # Connecting to Neo4j
    db = Neo4jCMS(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    #Loading the dataset
    data = pd.read_csv(DATASET)
    
    # Creating output directory
    output_dir = "/app/output"
    os.makedirs(output_dir, exist_ok=True)
    
    # DataFrame to store experiment results
    results = []

    # Running for different data sizes
    for size in [250000, 500000, 750000, 1000000]:
        print(f"Running experiments for {size} records...")
        df_subset = data.iloc[:size]
        
        # Creating the database and inserting data into the tables
        db.clear_database()
        db.insert_batch_data(df_subset)

        queries = {
            "Query 1": """
                MATCH (s:Student)-[:ENROLLED_IN]->(c:Course)
                WHERE c.course_name = 'Data Analysis'
                RETURN s.student_id, s.student_name
                LIMIT 10
            """,
            "Query 2": """
                MATCH (s:Student)-[:ENROLLED_IN]->(c:Course)
                WHERE c.course_name = 'Data Analysis'
                AND EXISTS {
                    MATCH (s)-[:SUBMITTED]->(a:Assignment)
                    WHERE a.submission_status = 'Yes'
                }
                RETURN s.student_id, s.student_name
                LIMIT 10
            """,
            "Query 3": """
                MATCH (s:Student)-[:SUBMITTED]->(a:Assignment)
                WHERE s.student_id IN [540214, 533994]
                SET a.submission_status = 'Yes', a.score = 30
                WITH s, a
                MATCH (s)-[:ENROLLED_IN]->(c:Course)
                RETURN s.student_id, s.student_name, c.course_name, a.assignment_id, a.submission_status, a.score
            """,
            "Query 4": """
                MATCH (s:Student)-[:ENROLLED_IN]->(c:Course)
                MATCH (s)-[:SUBMITTED]->(a:Assignment)
                WHERE c.course_name = 'Data Analysis'
                AND a.submission_status = 'Yes'
                AND a.score > 26
                RETURN s.student_id, s.student_name, c.course_name, a.submission_status, a.score
            """
        }

        for query_name, query in queries.items():
            print(f"Running {query_name} for {size} records...")
            execution_times, first_execution_time, avg_execution_time = db.run_experiments(query)
            
            # Adding results to the Dataframe
            results.append({
                "Records": size,
                "Query": query_name,
                "First Execution Time (ms)": first_execution_time,
                "Average Execution Time (ms)": avg_execution_time
            })
    
    # Saving results to an Excel file
    results_df = pd.DataFrame(results)
    output_file = os.path.join(output_dir, "neo4j_query_execution_times.xlsx")
    results_df.to_excel(output_file, index=False)
    print(f"Results saved to {output_file}")

    # Closing the Neo4j connection
    db.close()
    print("Data Insertion and Experiments completed successfully.")

if __name__ == "__main__":
    main()