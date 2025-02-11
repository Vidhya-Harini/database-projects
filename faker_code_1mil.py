from faker import Faker
import random
import csv

fake = Faker()

# Number of records
num_records = 1_000_000

# File output path
combined_file = '1_mil_records.csv'

def generate_unique_ids(count, start=100000):
    # Ensure that the range is large enough for the required count
    range_end = start + count
    if range_end > start + count:
        raise ValueError("The count exceeds the possible range.")
    return random.sample(range(start, range_end), count)

# Generate email address based on name
def generate_email(name):
    formatted_name = name.lower().replace(" ", "_")
    return f"{formatted_name}@gmail.com"

# List of course names and their corresponding 10 assignment titles
def generate_assignment_titles(course_name):
    titles = {
        "Artificial Intelligence & Robotics": [
            "Introduction to AI", "Machine Learning Basics", "Robotics and Control", "Neural Networks in AI", 
            "Deep Learning Models", "AI in Healthcare", "AI and Ethics", "AI Algorithms", 
            "Robotics in Manufacturing", "Future of Robotics"
        ],
        "Mechanical Engineering": [
            "Strength of Materials", "Thermodynamics Principles", "Fluid Mechanics", "Material Science",
            "Mechanical Vibrations", "Thermal Engineering", "Mechanisms and Machines", "Engineering Drawing",
            "Mechanical Design", "Manufacturing Processes"
        ],
        "Aerospace Engineering": [
            "Aerodynamics", "Space Exploration", "Flight Mechanics", "Aircraft Structures", 
            "Control Systems", "Spacecraft Design", "Rocket Propulsion", "Avionics Systems", 
            "Navigation and Guidance", "Aerospace Materials"
        ],
        "Supply Chain Management & Logistics": [
            "Logistics and Distribution", "Inventory Management", "Supply Chain Optimization", "Global Logistics", 
            "Transportation Management", "Warehousing", "Demand Planning", "Supplier Relationship Management", 
            "Risk Management", "Supply Chain Technologies"
        ],
        "International Business": [
            "Global Market Analysis", "Cross-cultural Management", "International Trade", "International Finance",
            "Global Business Strategy", "Emerging Markets", "Global Entrepreneurship", "Multinational Management",
            "Global Marketing", "International Negotiation"
        ],
        "Master of Business Administration (MBA)": [
            "Business Strategy", "Financial Management", "Marketing Principles", "Operations Management",
            "Human Resource Management", "Entrepreneurship", "Business Ethics", "Leadership Skills",
            "Global Business", "Strategic Management"
        ],
        "IT Management": [
            "IT Strategy", "Cybersecurity Management", "Information Systems", "Business Intelligence",
            "Enterprise Resource Planning", "Cloud Computing", "Data Governance", "Project Management",
            "Technology Innovation", "IT Operations"
        ],
        "Human Resource Management": [
            "Recruitment and Selection", "Employee Relations", "Labor Laws", "Training and Development",
            "Performance Management", "Organizational Behavior", "Workplace Diversity", "Compensation and Benefits",
            "HR Analytics", "Strategic HRM"
        ],
        "Architecture": [
            "Architectural Design", "Building Systems", "Construction Management", "Urban Design", 
            "Sustainability in Architecture", "Architectural Theory", "Materials and Construction", 
            "Structural Design", "Interior Architecture", "Digital Fabrication"
        ],
        "Interior Design": [
            "Design Principles", "Furniture Design", "Space Planning", "Lighting Design", "Sustainable Design",
            "Computer-Aided Design", "Residential Interiors", "Commercial Interiors", "Architectural Detailing",
            "Design Psychology"
        ],
        "Cybersecurity & Forensic Science": [
            "Network Security", "Cryptography", "Digital Forensics", "Incident Response", "Cybercrime Investigation",
            "Cyber Law", "Risk Management", "Ethical Hacking", "Malware Analysis", "Cybersecurity Technologies"
        ],
        "Emergency & Disaster Management": [
            "Disaster Preparedness", "Crisis Management", "Risk Assessment", "Public Health Emergency",
            "Incident Command Systems", "Search and Rescue", "Emergency Response Coordination", 
            "Disaster Recovery", "Community Resilience", "Humanitarian Assistance"
        ],
        "Computer Science": [
            "Data Structures", "Algorithms", "Operating Systems", "Software Engineering", 
            "Database Systems", "Computer Networks", "Web Development", "Machine Learning", 
            "Artificial Intelligence", "Mobile Application Development"
        ],
        "Information Technology": [
            "Networking Fundamentals", "Web Technologies", "Database Management", "System Analysis", 
            "Project Management", "Security in IT", "Cloud Computing", "Data Warehousing", 
            "E-Commerce", "Software Development"
        ],
        "Artificial Intelligence & Machine Learning": [
            "Introduction to Machine Learning", "Supervised Learning", "Unsupervised Learning", 
            "Neural Networks", "Deep Learning", "Reinforcement Learning", "Natural Language Processing", 
            "Computer Vision", "AI Applications", "AI Ethics"
        ],
        "Data Analysis": [
            "Statistical Analysis", "Regression Analysis", "Data Cleaning", "Exploratory Data Analysis", 
            "Data Visualization", "Machine Learning in Data Analysis", "Big Data", "Time Series Analysis",
            "Predictive Modeling", "Data Analysis with Python"
        ],
        "Cyber Security": [
            "Network Security", "Security Protocols", "Cryptography", "Risk Management", 
            "Ethical Hacking", "Security Operations", "Malware Analysis", "Penetration Testing", 
            "Security Policy", "Cyber Threat Intelligence"
        ],
        "Cloud Computing": [
            "Cloud Architecture", "Virtualization", "Cloud Security", "Cloud Services", 
            "Distributed Systems", "Cloud Storage", "Cloud-based Applications", "Cloud Computing Platforms",
            "Data Center Management", "Cloud Computing Security"
        ],
        "Hospitality Management": [
            "Hotel Management", "Food and Beverage Management", "Event Planning", "Hospitality Marketing",
            "Tourism Management", "Guest Services", "Hospitality Operations", "Sustainable Tourism", 
            "Hospitality Law", "Leadership in Hospitality"
        ],
        "Tourism & Leisure": [
            "Tourism Planning", "Tourism Economics", "Cultural Heritage", "Tourism Marketing", 
            "Tourism Destination Management", "Sustainable Tourism", "Tourism Policy", "Leisure Management",
            "Event Planning", "Tourism Research"
        ],
        "Psychology": [
            "Introduction to Psychology", "Behavioral Psychology", "Cognitive Psychology", "Developmental Psychology",
            "Clinical Psychology", "Psychological Testing", "Neuropsychology", "Social Psychology",
            "Psychopathology", "Psychotherapy"
        ],
        "Medicine": [
            "Anatomy and Physiology", "Medical Ethics", "Pathology", "Pharmacology", 
            "Clinical Skills", "Public Health", "Medical Microbiology", "Immunology", 
            "Medical Research", "Medical Imaging"
        ],
        "Political Science and International Relations": [
            "International Relations Theory", "Political Systems", "Comparative Politics", "Public Policy", 
            "Global Governance", "International Law", "Political Economy", "Conflict Resolution", 
            "International Organizations", "Foreign Policy Analysis"
        ],
        "Nursing": [
            "Nursing Fundamentals", "Clinical Nursing", "Healthcare Ethics", "Nursing Research", 
            "Pharmacology for Nurses", "Pediatric Nursing", "Adult Nursing", "Psychiatric Nursing", 
            "Community Health Nursing", "Geriatric Nursing"
        ],
        "International Law": [
            "International Legal Systems", "Human Rights Law", "International Trade Law", 
            "International Humanitarian Law", "Dispute Resolution", "International Criminal Law",
            "Diplomacy and Law", "International Arbitration", "Sovereignty", "International Environmental Law"
        ],
        "Criminal Justice": [
            "Criminal Law", "Criminal Procedure", "Forensic Science", "Crime and Society", 
            "Policing", "Corrections", "Criminal Investigations", "Juvenile Justice", 
            "Ethics in Criminal Justice", "Victimology"
        ],
        "Economics": [
            "Microeconomics", "Macroeconomics", "International Economics", "Development Economics", 
            "Economic Theory", "Public Finance", "Labor Economics", "Environmental Economics", 
            "Behavioral Economics", "Econometrics"
        ],
        "Forensic Psychology": [
            "Criminal Behavior", "Psychological Assessment", "Mental Health Law", "Psychopathology", 
            "Criminal Profiling", "Jury Decision Making", "Psychological Testing", "Forensic Interviewing", 
            "Risk Assessment", "Violent Crime"
        ],
        "Archaeology": [
            "Prehistoric Archaeology", "Historical Archaeology", "Field Methods", "Ethnoarchaeology", 
            "Archaeological Theory", "Cultural Resource Management", "Archaeological Excavation", 
            "Ancient Civilizations", "Material Culture", "Archaeological Science"
        ],
        "Sociology": [
            "Introduction to Sociology", "Social Theory", "Social Problems", "Cultural Sociology", 
            "Sociological Research", "Gender Studies", "Sociology of Education", "Race and Ethnicity",
            "Urban Sociology", "Criminology"
        ]
    }
    
    return titles.get(course_name, [])

# Generate course content based on course name
def generate_course_content(course_name):
    content = {
        "Artificial Intelligence & Robotics": "Principles of AI, robotics, machine learning, and their applications.",
        "Mechanical Engineering": "Design and analysis of mechanical systems, thermodynamics, and materials science.",
        "Aerospace Engineering": "Study of aerodynamics, spacecraft design, propulsion systems, and flight mechanics.",
        "Supply Chain Management & Logistics": "Methods for optimizing supply chains, logistics, and distribution systems.",
        "International Business": "Study of global business, international markets, and strategies for operating worldwide.",
        "Master of Business Administration (MBA)": "An integrated study of management principles for aspiring business leaders.",
        "IT Management": "Managing IT infrastructure, strategies, and systems in organizations.",
        "Human Resource Management": "Focus on recruitment, employee management, and organizational behavior.",
        "Architecture": "Study of building design, structural engineering, and sustainable architecture.",
        "Interior Design": "Designing functional and aesthetic interiors for residential and commercial spaces.",
        "Cybersecurity & Forensic Science": "Study of digital forensics, network security, and crime scene investigation techniques.",
        "Emergency & Disaster Management": "Planning and coordination for disaster prevention, response, and recovery.",
        "Computer Science": "Fundamentals of programming, algorithms, data structures, and computer architecture.",
        "Information Technology": "Application and management of IT systems, networks, and security.",
        "Artificial Intelligence & Machine Learning": "Foundations and advanced concepts in AI, machine learning, and neural networks.",
        "Data Analysis": "Techniques for analyzing and interpreting data to uncover trends and insights.",
        "Cyber Security": "Study of security technologies, ethical hacking, and techniques to protect against cyber threats.",
        "Cloud Computing": "Exploring the architecture, services, and deployment models of cloud computing.",
        "Hospitality Management": "Operations, marketing, and management of the hospitality industry.",
        "Tourism & Leisure": "Study of tourism systems, leisure management, and the business of travel.",
        "Psychology": "The study of human behavior, cognition, emotions, and mental health.",
        "Medicine": "Healthcare practices, patient care, and clinical applications in medicine.",
        "Political Science and International Relations": "Study of political systems, international organizations, and governance.",
        "Nursing": "Study of patient care, nursing practices, and the healthcare system.",
        "International Law": "Global legal systems, human rights, and international trade law.",
        "Criminal Justice": "Study of crime, law enforcement, legal systems, and the criminal justice process.",
        "Economics": "Theory and practice of economics, market behaviors, and economic policies.",
        "Forensic Psychology": "Psychological aspects of criminal behavior, profiling, and legal proceedings.",
        "Archaeology": "Exploring ancient cultures through material evidence and archaeological methods.",
        "Sociology": "Examination of social behavior, groups, and societal issues."
    }
    return content.get(course_name, "Content not available for this course.")

def generate_all_data():
    student_ids = generate_unique_ids(num_records)
    professor_ids = generate_unique_ids(num_records // 10)
    assignment_ids = generate_unique_ids(num_records // 10)
    
    # List of all course names
    course_names = [
        "Artificial Intelligence & Robotics", "Mechanical Engineering", "Aerospace Engineering",
        "Supply Chain Management & Logistics", "International Business", "Master of Business Administration (MBA)",
        "IT Management", "Human Resource Management", "Architecture", "Interior Design", 
        "Cybersecurity & Forensic Science", "Emergency & Disaster Management", "Computer Science", 
        "Information Technology", "Artificial Intelligence & Machine Learning", "Data Analysis",
        "Cyber Security", "Cloud Computing", "Hospitality Management", "Tourism & Leisure", 
        "Psychology", "Medicine", "Political Science and International Relations", "Nursing", 
        "International Law", "Criminal Justice", "Economics", "Forensic Psychology", 
        "Archaeology", "Sociology"
    ]

    # Assigning Vidhya Harini and Nuthan Puli to specific IDs
    student_data = [
        {"name": "Vidhya Harini", "student_id": 533994, "course_name": "Data Analysis"},
        {"name": "Nuthan Puli", "student_id": 540214, "course_name": "Data Analysis"}
    ]

    # Assigning Armando Ruggeri to the Data Analysis course with a fixed ID
    professor_data = [
        {"professor_name": "Armando Ruggeri", "professor_id": 676734, "course_name": "Data Analysis"}
    ]

    print("Starting to generate data...")

    with open(combined_file, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Insert the specific records for Vidhya Harini and Nuthan Puli
        for student in student_data:
            student_id = student["student_id"]
            student_name = student["name"]
            student_email_address = generate_email(student_name)
            
            # Get a random course_name from the course_names list
            course_name = student["course_name"]
            course_content = generate_course_content(course_name)
            course_id = 100015
            
            professor = professor_data[0]  # Armando Ruggeri for Data Analysis
            professor_id = professor["professor_id"]
            professor_name = professor["professor_name"]
            professor_email_address = generate_email(professor_name)
            
            # Assignments and submission status
            assignment_title = "Database: Course Management System"  # Fixed title for both students
            assignment_id = random.choice(assignment_ids)
            submission_status = "No"  # Submission status for Vidhya Harini and Nuthan Puli
            score = 0  # Score is 0 for "No" submission
            
            # Write header only once
            if f.tell() == 0:  # Check if file is empty before writing header
                writer.writerow([ 
                    'course_id', 'course_name', 'course_content',
                    'student_id', 'student_name', 'student_email_address',
                    'professor_id', 'professor_name', 'professor_email_address',
                    'assignment_id', 'assignment_title', 'submission_status', 'score'
                ])
            
            # Write record for each student
            writer.writerow([ 
                course_id, course_name, course_content,
                student_id, student_name, student_email_address,
                professor_id, professor_name, professor_email_address,
                assignment_id, assignment_title, submission_status, score
            ])
        
        # Generate remaining records for other students and courses (if needed)
        for i in range(num_records):
            student_id = student_ids[i]
            professor_id = professor_ids[i % len(professor_ids)]
            
            # Randomly pick a course name and its content
            course_name = random.choice(course_names)
            course_id = 100000 + course_names.index(course_name)  # Assigning unique ID
            
            student_name = fake.name()
            professor_name = fake.name()

            # Handle assignments
            assignment_titles = generate_assignment_titles(course_name)
            assignment_title = random.choice(assignment_titles)
            assignment_id = assignment_ids[i % len(assignment_ids)]
            submission_status = random.choice(['Yes', 'No'])
            score = random.randint(18, 30) if submission_status == "Yes" else 0

            course_content = generate_course_content(course_name)

            # Write the record
            writer.writerow([ 
                course_id, course_name, course_content,
                student_id, student_name, generate_email(student_name),
                professor_id, professor_name, generate_email(professor_name),
                assignment_id, assignment_title, submission_status, score
            ])

if __name__ == "__main__":
    generate_all_data()
    print("Data generation completed for 1,000,000 records.")