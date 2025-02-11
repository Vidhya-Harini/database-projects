import time
import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "course_management_system"

dataset = '1_mil_records.csv'
NUM_EXPERIMENTS = 31

# Function to create the database
def create_database(client):
    db = client[DATABASE_NAME]
    print(f"Database '{DATABASE_NAME}' created or already exists.")
    return db

# Function to create and insert the data into each table using BatchProcessing
def insert_data(db, df, batch_size=10000):
    # Courses collection
    courses = db['Courses']
    courses_data = df[['course_id', 'course_name', 'course_content']].to_dict(orient='records')
    for i in range(0, len(courses_data), batch_size):
        courses.insert_many(courses_data[i:i + batch_size])

    # Students collection
    students = db['Students']
    students_data = df[['student_id', 'student_name', 'student_email_address', 'course_id']].to_dict(orient='records')
    for i in range(0, len(students_data), batch_size):
        students.insert_many(students_data[i:i + batch_size])

    # Professors collection
    professors = db['Professors']
    professors_data = df[['professor_id', 'professor_name', 'professor_email_address', 'course_id']].to_dict(orient='records')
    for i in range(0, len(professors_data), batch_size):
        professors.insert_many(professors_data[i:i + batch_size])

    # Assignments collection
    assignments = db['Assignments']
    assignments_data = df[['assignment_id', 'assignment_title', 'submission_status', 'score', 'student_id', 'course_id']].to_dict(orient='records')
    for i in range(0, len(assignments_data), batch_size):
        assignments.insert_many(assignments_data[i:i + batch_size])

# Function to create indexes for each primary key in each tables
def create_indexes(db):
    db.Students.create_index([("student_id", 1)])
    db.Courses.create_index([("course_id", 1)])
    db.Assignments.create_index([("student_id", 1), ("assignment_id", 1)])
    db.Professors.create_index([("professor_id", 1)])
    print("Indexes created.")

# Function to run the query and measure the execution times
def run_query(query_func):
    start_time = time.time()
    results = query_func()
    end_time = time.time()
    return (end_time - start_time) * 1000  # return time in milliseconds

# Function to get first and average execution times
def run_experiments(query_func, num_experiments=NUM_EXPERIMENTS):
    first_execution_time = run_query(query_func)
    execution_times = [first_execution_time]  # Storing the first execution time separately

    # Running the query 30 more times to calculate it's average
    for _ in range(num_experiments - 1):
        execution_time = run_query(query_func)
        execution_times.append(execution_time)

    avg_execution_time = np.mean(execution_times[1:])  # Average of the 30 execution times
    return execution_times, first_execution_time, avg_execution_time

# Function to return a dictionary of query's
def get_query_functions(db):

    return {
        "Query 1": lambda: list(db.Students.aggregate([  # Query 1
            {"$lookup": {"from": "Courses", "localField": "course_id", "foreignField": "course_id", "as": "course"}},
            {"$unwind": "$course"},
            {"$match": {"course.course_name": "Data Analysis"}},
            {"$project": {"student_id": 1, "student_name": 1}},
            {"$limit": 10}
        ])),

        "Query 2": lambda: list(db.Students.aggregate([  # Query 2
            {"$lookup": {"from": "Courses", "localField": "course_id", "foreignField": "course_id", "as": "course"}},
            {"$unwind": "$course"},
            {"$match": {
                "course.course_name": "Data Analysis",
                "student_id": {
                    "$in": list(db.Assignments.find({"submission_status": "yes"}, {"student_id": 1}))  # Fixes `$in`
                }
            }},
            {"$project": {"student_id": 1, "student_name": 1}},
            {"$limit": 10}
        ])),

        "Query 3": lambda: (  # Query 3
            db.Students.aggregate([
                {"$match": {"student_id": {"$in": [540214, 533994]}}},
                {"$lookup": { 
                    "from": "Courses", 
                    "localField": "course_id", 
                    "foreignField": "course_id", 
                    "as": "course" 
                }},
                {"$lookup": { 
                    "from": "Assignments", 
                    "localField": "student_id", 
                    "foreignField": "student_id", 
                    "as": "assignments" 
                }},
                {"$unwind": "$course" },
                {"$unwind": "$assignments" },
                {"$project": {
                    "_id": 0,
                    "student_id": 1,
                    "student_name": 1,
                    "course_id": 1,
                    "course_name": "$course.course_name",
                    "assignment_id": "$assignments.assignment_id",
                    "submission_status": "$assignments.submission_status",
                    "score": "$assignments.score"
                }},
                {"$group": {
                    "_id": { 
                        "student_id": "$student_id", 
                        "assignment_id": "$assignment_id" 
                    },
                    "student_id": {"$first": "$student_id" },
                    "student_name": {"$first": "$student_name" },
                    "course_id": {"$first": "$course_id" },
                    "course_name": {"$first": "$course_name" },
                    "assignment_id": {"$first": "$assignment_id" },
                    "submission_status": {"$first": "$submission_status" },
                    "score": {"$first": "$score" }
                }},
                {"$sort": {"student_id": 1, "assignment_id": 1 }}
            ])
        ),

        "Query 4": lambda: (  # Query 4
            db.Students.aggregate([
                { "$lookup": {  
                    "from": "Courses",
                    "localField": "course_id",
                    "foreignField": "course_id",
                    "as": "course"
                }},
                { "$unwind": "$course" },

                { "$lookup": {  
                    "from": "Assignments",
                    "localField": "student_id",
                    "foreignField": "student_id",
                    "as": "assignments"
                }},
                { "$unwind": { "path": "$assignments", "preserveNullAndEmptyArrays": True } },

                { "$match": {  
                    "course.course_name": "Data Analysis",
                    "assignments.submission_status": "Yes",
                    "assignments.score": { "$gt": 26 }
                }},

                { "$project": {  
                    "_id": 0,
                    "student_id": 1,
                    "student_name": 1,
                    "course_id": 1,
                    "course_name": "$course.course_name",
                    "submission_status": "$assignments.submission_status",
                    "score": "$assignments.score"
                }}
            ])
        )
    }

# Function to clear the collections instead of dropping the whole database
def clear_collections(db):
    db.Courses.delete_many({})
    db.Students.delete_many({})
    db.Professors.delete_many({})
    db.Assignments.delete_many({})
    print("Collections cleared.")

# Function to run the queries parallely using ThreadPoolExecutor
def run_queries_parallel(query_functions, db):
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda query_func: run_experiments(query_func), query_functions.values()))
    return results

# Main Function
def main():
    client = MongoClient(MONGO_URI)
    db = create_database(client)

    # Loading the dataset
    data = pd.read_csv(dataset)

    # Creating output directory
    output_dir = "/app/output"
    os.makedirs(output_dir, exist_ok=True)

    # DataFrame to store experiment results
    results = []

    # Running for different data sizes
    for size in [250000, 500000, 750000, 1000000]:
        print(f"Running experiments for {size} records...")
        df_subset = data.iloc[:size]

        # Inserting data into MongoDB
        insert_data(db, df_subset)

        # Creating indexes
        create_indexes(db)

        # Queries
        query_functions = get_query_functions(db)

        # Running experiments for each query
        for query_name, query_func in query_functions.items():
            print(f"Running {query_name} for {size} records...")
            execution_times, first_execution_time, avg_execution_time = run_experiments(query_func)

            # Adding results to the DataFrame
            results.append({
                "Records": size,
                "Query": query_name,
                "First Execution Time (ms)": first_execution_time,
                "Average Execution Time (ms)": avg_execution_time
            })

        # Clearing collections after each experiment
        clear_collections(db)

    # Save results to an Excel file
    output_file = os.path.join(output_dir, "mongodb_query_execution_times.xlsx")
    results_df = pd.DataFrame(results)
    results_df.to_excel(output_file, index=False)
    print(f"Results saved to {output_file}")

    # Closing the MongoDB connection
    client.close()
    print("Experiments completed successfully.")


if __name__ == "__main__":
    main()