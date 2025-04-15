
**Description
Ingestor is a data ingestion tool designed to facilitate seamless data transfer between ClickHouse and flat files (e.g., CSV). The project features a Java Spring Boot backend for handling data operations and a React frontend for user interaction, enabling users to test ClickHouse connections, retrieve table schemas, and manage file-based data ingestion.
Prerequisites**

```
Docker: For running ClickHouse server or other containerized services.
JDK 17 or higher: For running the Spring Boot backend.
Maven: For dependency management and building the backend.
Node.js (v16 or higher): For running the React frontend.
ClickHouse Instance: Access to a ClickHouse database (local or remote).
ClickHouse JDBC Driver: For database connectivity.
```

# **Installation**

```
Clone the Repository
git clone https://github.com/jitu7033/ingestor.git
cd ingestor
```


# **Backend Setup**
```
# Install dependencies
mvn clean install
```


# **Frontend Setup**
```
# Navigate to frontend directory
cd frontend
# Install dependencies
npm install
```


```
Configure ClickHouse Connection

Update application.properties in backend/src/main/resources with your ClickHouse connection details:
spring.datasource.url=jdbc:clickhouse://localhost:8123/default
spring.datasource.username=default
spring.datasource.password=

```





# **Usage**

```
Run the Backend
mvn spring-boot:run
```


# The backend will be available at http://localhost:8080.



# **Run the Frontend**
```
cd frontend
npm start
```


# The React app will be available at http://localhost:3000.


# Access the Application

```
Open your browser and navigate to http://localhost:3000.
Use the interface to test ClickHouse connections, view table schemas, or ingest/export data to/from flat files.
```



# **Features**

```
Connection Testing: Verify connectivity to ClickHouse instances.
Schema Retrieval: Fetch and display table schemas from ClickHouse.
Data Ingestion: Import data from CSV files to ClickHouse tables.
Data Export: Export ClickHouse table data to CSV files.
User Interface: Intuitive React-based frontend for managing ingestion tasks.
```

# **Technology Stack**

```
Backend: Java, Spring Boot, ClickHouse JDBC Driver
Frontend: React, Tailwind CSS
Database: ClickHouse
File Handling: Custom CSV parsing with OpenCSV
```

# **(Contributing)**

```
Fork the repository.
Create a feature branch (git checkout -b feature/YourFeature).
Commit your changes (git commit -m 'Add YourFeature').
Push to the branch (git push origin feature/YourFeature).
Open a pull request.
```
