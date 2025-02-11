import pandas as pd
import redis
import time
import os
import numpy as np

# Redis connection
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

dataset = '1_mil_records.csv'
NUM_EXPERIMENTS = 31

# Function to connect to Redis
def connect_to_db():
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    return r

# Function to insert data with indexing
def insert_batch_data(r, df, batch_size=1000):
    pipeline = r.pipeline()
    
    for i, row in df.iterrows():
        # Inserting Courses data
        pipeline.hset(f"course:{row['course_id']}", mapping={"course_name": row['course_name'], "course_content": row['course_content']})
        
        # Inserting Students data
        pipeline.hset(f"student:{row['student_id']}", mapping={
            "student_name": row['student_name'], 
            "student_email_address": row['student_email_address'], 
            "course_id": row['course_id']
        })
        # Indexing students by course
        pipeline.sadd(f"course:{row['course_id']}:students", row['student_id'])
        
        # Inserting Professors data
        pipeline.hset(f"professor:{row['professor_id']}", mapping={
            "professor_name": row['professor_name'], 
            "professor_email_address": row['professor_email_address'], 
            "course_id": row['course_id']
        })
        
        # Inserting Assignments data
        pipeline.hset(f"assignment:{row['assignment_id']}", mapping={
            "assignment_title": row['assignment_title'], 
            "submission_status": row['submission_status'], 
            "score": row['score'], 
            "student_id": row['student_id'], 
            "course_id": row['course_id']
        })
        # Indexing assignments by student and score
        pipeline.sadd(f"student:{row['student_id']}:assignments", row['assignment_id'])
        pipeline.zadd(f"course:{row['course_id']}:scores", {row['assignment_id']: row['score']})

        # Executing batch every `batch_size` records
        if (i + 1) % batch_size == 0:
            pipeline.execute()

    # Executing any remaining commands after the loop
    pipeline.execute()

# Function to run the query and measure execution times
def run_query(r, query_func):
    start_time = time.time()
    query_func(r)
    end_time = time.time()
    return (end_time - start_time) * 1000  # return time in milliseconds

# Function to get first and average execution times
def run_experiments(r, query_func, num_experiments=NUM_EXPERIMENTS):
    first_execution_time = run_query(r, query_func)
    execution_times = [first_execution_time]  # Store the first execution time separately

    # Run the query 30 more times to calculate it's average
    for _ in range(num_experiments - 1):
        execution_time = run_query(r, query_func)
        execution_times.append(execution_time)

    avg_execution_time = np.mean(execution_times[1:])  # Average of the 30 execution times
    return execution_times, first_execution_time, avg_execution_time

# Query functions for Redis
def query_1(r):
    # Query 1: Getting students in "Data Analysis" course
    course_keys = [key for key in r.keys("course:*") if r.type(key) == "hash"]
    course_id = next((key.split(":")[1] for key in course_keys if r.hget(key, "course_name") == "Data Analysis"), None)
    if course_id:
        student_ids = r.smembers(f"course:{course_id}:students")
        students = [{"student_id": sid, "student_name": r.hget(f"student:{sid}", "student_name")} for sid in student_ids]
        return students[:10]
    return []

def query_2(r):
    # Query 2: Getting students in "Data Analysis" course with submission status 'yes'
    course_keys = [key for key in r.keys("course:*") if r.type(key) == "hash"]
    course_id = next((key.split(":")[1] for key in course_keys if r.hget(key, "course_name") == "Data Analysis"), None)
    if course_id:
        student_ids = r.smembers(f"course:{course_id}:students")
        students = []
        for sid in student_ids:
            assignment_ids = r.smembers(f"student:{sid}:assignments")
            for aid in assignment_ids:
                if r.hget(f"assignment:{aid}", "submission_status") == "yes":
                    students.append({"student_id": sid, "student_name": r.hget(f"student:{sid}", "student_name")})
                    break
        return students[:10]
    return []

def query_3(r):
    # Query 3: Updating submission_status and score, then fetching students and assignments
    for student_id in ["540214", "533994"]:
        assignment_ids = r.smembers(f"student:{student_id}:assignments")
        for aid in assignment_ids:
            r.hset(f"assignment:{aid}", mapping={"submission_status": "Yes", "score": 30})

    # Fetching updated student and assignment data
    result = []
    for student_id in ["540214", "533994"]:
        student_key = f"student:{student_id}"
        course_id = r.hget(student_key, "course_id")
        course_name = r.hget(f"course:{course_id}", "course_name")
        assignment_ids = r.smembers(f"student:{student_id}:assignments")
        for aid in assignment_ids:
            result.append({
                "student_id": student_id,
                "student_name": r.hget(student_key, "student_name"),
                "course_id": course_id,
                "course_name": course_name,
                "submission_status": r.hget(f"assignment:{aid}", "submission_status"),
                "score": r.hget(f"assignment:{aid}", "score")
            })
    return result

def query_4(r):
    # Query 4: Getting students with submission_status 'yes' and score > 26 in "Data Analysis"
    course_keys = [key for key in r.keys("course:*") if r.type(key) == "hash"]
    course_id = next((key.split(":")[1] for key in course_keys if r.hget(key, "course_name") == "Data Analysis"), None)
    if course_id:
        assignment_ids = r.zrangebyscore(f"course:{course_id}:scores", 27, "+inf")
        result = []
        for aid in assignment_ids:
            if r.hget(f"assignment:{aid}", "submission_status") == "yes":
                student_id = r.hget(f"assignment:{aid}", "student_id")
                result.append({
                    "student_id": student_id,
                    "student_name": r.hget(f"student:{student_id}", "student_name"),
                    "course_id": course_id,
                    "course_name": r.hget(f"course:{course_id}", "course_name"),
                    "submission_status": r.hget(f"assignment:{aid}", "submission_status"),
                    "score": r.hget(f"assignment:{aid}", "score")
                })
        return result
    return []

# Main function
def main():
    # Loading dataset
    data = pd.read_csv(dataset)

    # Connecting to Redis
    r = connect_to_db()
    
    # Creating output directory
    output_dir = "/app/output"
    os.makedirs(output_dir, exist_ok=True)

    # DataFrame to store experiment results
    results = []

    # Run experiments for different sizes
    for i, size in enumerate([250000, 500000, 750000, 1000000]):
        print(f"Running experiments for {size} records...")

        # Clearing Redis database before inserting new records
        r.flushdb()
        print(f"Redis database cleared.")

        df_subset = data.iloc[:size]

        # Inserting batch data into Redis
        insert_batch_data(r, df_subset)

        # Running experiments for each query
        for query_name, query_func in {
            "query_1": query_1,
            "query_2": query_2,
            "query_3": query_3,
            "query_4": query_4
        }.items():
            print(f"Running {query_name} for {size} records...")
            execution_times, first_execution_time, avg_execution_time = run_experiments(r, query_func)

            # Adding results to the DataFrame
            results.append({
                "Records": size,
                "Query": query_name,
                "First Execution Time (ms)": first_execution_time,
                "Average Execution Time (ms)": avg_execution_time
            })

        print(f"Experiment for {size} records completed.")

    # Saving results to an Excel file
    output_file = os.path.join(output_dir, "redis_query_execution_times.xlsx")
    results_df = pd.DataFrame(results)
    results_df.to_excel(output_file, index=False)
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()