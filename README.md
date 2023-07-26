**Syspad - v1.1.0**

**Description**

The Database Monitor Agent is an application developed in Flask, a web development framework in Python, that aims to monitor and record insertions, updates, and deletions operations in a specific database.

**Key Features**

- Agent start
- Agent database start
- Agent verification start

**Complete Documentation**

The complete API documentation, including details about all available endpoints, request parameters, response formats, and usage examples, can be found at [DOCUMENTATION LINK](https://github.com/FRIDA-LACNIC-UECE/documentation/blob/main/SysPAD%20Documentation.pdf) or SWAGGER DOCUMENTATION at http://localhost:3000 after executed.

**Technologies Used**

- Programming Language: Python (Version 3.10)
- Framework/API/Web Framework: Flask Framework
- Supported Database: MySQL and PostgreSQL
- Object Relational Mapping: SqlAlchemy

**Installation and Usage**

**Without docker:**  

1. Clone this repository to your local machine using the following command:

   ```
   git clone https://github.com/FRIDA-LACNIC-UECE/agent.git
   ```

2. Navigate to the project directory:

   ```
   cd agent
   ```

3. Install the necessary dependencies:

   ```
   pip3 install -r requirements.txt
   ```

4. Start the server:

   ```
   python3 appplication.py
   ```

**With docker:** 

1. Start the docker compose:

   ```
   docker compose up
   ```

**Contributing**

If you would like to contribute to the project, please follow these steps:

1. Fork this repository.
2. Create a branch for your feature (`git checkout -b feature/your-feature`).
3. Commit your changes (`git commit -m 'Add your feature'`).
4. Push the branch (`git push origin feature/your-feature`).
5. Open a pull request.
