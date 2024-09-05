# Real-Time Data Extraction from KoboToolbox

![ETL Diagram](/assets/ETL.png)

## Overview

This project implements a solution to extract data from KoboToolbox, save it in a MySQL database, and handle real-time updates. The solution is built using Python, Flask, and MySQL, and it includes a webhook for real-time data processing and a periodic data extraction script.

## Features 

- **Real-Time Data Updates**: Automatically update the database with new or modified records via a Flask webhook.
- **Scheduled Data Extraction**: Use a cron job to periodically extract data from KoboToolbox, ensuring no records are missed.
- **Database Integrity**: Data is stored in a normalized schema, preventing duplicates and maintaining consistency.
- **Error Handling & Logging**: Robust error handling and logging throughout the extraction and saving processes.

## Prerequisites

- Python 3.7 or higher
- MySQL 5.7 or higher
- pip (Python package manager)
- Virtual environment tool (`venv` or `virtualenv`)

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/danielpiyo/kobo_inkomoko_task.git
cd kobo_inkomoko_task
```
 
### 2. Create and Activate a Virtual Environment

```bash
python3 -m venv kobo_env
source kobo_env/bin/activate 
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up the MySQL Database

- Create a MySQL database:
  
  ```sql
  CREATE DATABASE kobo_data_db;
  USE kobo_data_db;
  ```

- Run the provided SQL script to create the tables:
  
  ```sql
         -- Creating the surveys table
        CREATE TABLE surveys (
            survey_id INT AUTO_INCREMENT PRIMARY KEY,
            uuid VARCHAR(255) NOT NULL UNIQUE,
            starttime DATETIME NOT NULL,
            endtime DATETIME NOT NULL,
            cd_survey_date VARCHAR(50) NOT NULL,
            meta_instanceID VARCHAR(255) NOT NULL,
            xform_id_string VARCHAR(255) NOT NULL,
            submission_time DATETIME NOT NULL,
            status VARCHAR(50),
            INDEX (uuid),  -- Index to speed up lookups by UUID
            INDEX (xform_id_string)  -- Index to speed up lookups by form ID
        );
        
        -- Creating the section_a table
        CREATE TABLE section_a (
            id INT AUTO_INCREMENT PRIMARY KEY,
            survey_id INT NOT NULL,
            unique_id VARCHAR(255) NOT NULL UNIQUE,
            cd_biz_country_name VARCHAR(255) NOT NULL,
            cd_biz_region_name VARCHAR(255) NOT NULL,
            FOREIGN KEY (survey_id) REFERENCES surveys(survey_id) ON DELETE CASCADE,
            INDEX (survey_id),  -- Index to optimize joins on survey_id
            INDEX (unique_id)  -- Index to ensure uniqueness and speed up lookups
        );
        
        -- Creating the section_b table
        CREATE TABLE section_b (
            id INT AUTO_INCREMENT PRIMARY KEY,
            survey_id INT NOT NULL,
            bda_name VARCHAR(255) NOT NULL,
            cd_cohort VARCHAR(255) NOT NULL,
            cd_program VARCHAR(255) NOT NULL,
            FOREIGN KEY (survey_id) REFERENCES surveys(survey_id) ON DELETE CASCADE,
            INDEX (survey_id)  -- Index to optimize joins on survey_id
        );
        
        -- Creating the section_c table
        CREATE TABLE section_c (
            id INT AUTO_INCREMENT PRIMARY KEY,
            survey_id INT NOT NULL,
            cd_client_name VARCHAR(255) NOT NULL,
            cd_client_id_manifest VARCHAR(255) NOT NULL UNIQUE,
            cd_location VARCHAR(255) NOT NULL,
            cd_clients_phone VARCHAR(255) NOT NULL,
            cd_clients_phone_smart_feature VARCHAR(255) NOT NULL,
            cd_gender VARCHAR(50) NOT NULL,
            cd_age INT NOT NULL,
            cd_nationality VARCHAR(255) NOT NULL,
            cd_strata VARCHAR(255) NOT NULL,
            cd_disability VARCHAR(50) NOT NULL,
            cd_education VARCHAR(255) NOT NULL,
            cd_client_status VARCHAR(255) NOT NULL,
            cd_sole_income_earner VARCHAR(50) NOT NULL,
            cd_howrespble_pple INT NOT NULL,
            FOREIGN KEY (survey_id) REFERENCES surveys(survey_id) ON DELETE CASCADE,
            INDEX (survey_id),  -- Index to optimize joins on survey_id
            INDEX (cd_client_id_manifest)  -- Index to ensure uniqueness and speed up lookups
        );
        
        -- Creating the business_status table
        CREATE TABLE business_status (
            id INT AUTO_INCREMENT PRIMARY KEY,
            survey_id INT NOT NULL,
            cd_biz_status VARCHAR(255) NOT NULL,
            bd_biz_operating VARCHAR(50) NULL,
            FOREIGN KEY (survey_id) REFERENCES surveys(survey_id) ON DELETE CASCADE,
            INDEX (survey_id)  -- Index to optimize joins on survey_id
        );
  ```

- Update the database connection details in `config.py` with reference to how you set up the environment variables.

```python
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import os
    
    # Database configuration
    DATABASE_URI = os.getenv('DB_URL')
    # KoboToolbox API configuration
    KOBO_API_URL = os.getenv('KOBO_URL')
    KOBO_API_HEADERS = {
        'Authorization': os.getenv('KOBO_HEADER_TOKEN'),
        'Cookie': 'django_language=en'
    }
    # Setup the MySQL database engine 
    engine = create_engine(DATABASE_URI)
    Session = sessionmaker(bind=engine)

 ```

### 5. Set Up the Flask Webhook

- Update the webhook URL in `app.py` 
- Run the Flask app:

  ```bash
  python app.py
  ```

- The webhook will be accessible at `http:// 162.254.34.181:3000/api/webhook` or your deployed URL.

### 6. Register the Webhook with KoboToolbox

Use the provided script to register your webhook with KoboToolbox:

```python
import requests
import json

webhook_url = "http:// 162.254.34.181:3000/api/webhook"
url = "http://dev.inkomoko.com:1055/register_webhook"
payload = json.dumps({"url": webhook_url})
headers = {'Content-Type': 'application/json'}
response = requests.post(url, headers=headers, data=payload)
print(response.text)
```

### 7. Set Up a Cron Job (Optional)

For periodic data extraction:

```bash
crontab -e
# To run the script every 20 min just for test 
*/20 * * * * /usr/bin/python3 /home/flask/kobo_env/data_pipeline.py
```

Alternatively, run the ETL script (data_pipeline.py)

```bash
python data_pipeline.py
```
Note: 
Update your .bashrc or .env file with these environment variables. For securety we keep them in environment Update your environment variables as shown in the documentation shared:
export DB_NAME=''
export DB_URL=''
export TEST_DB_URL=''
export WEB_HK_API_ENDP=''
export WEB_HK_REG_URL=''
export KOBO_URL=''
export KOBO_HEADER_TOKEN=''

## Usage

### Submitting Data

1. Access the KoboToolbox form via the provided link -- https://ee.kobotoolbox.org/x/Ea06s3dE..
2. Enter the sample UIDs in the "Enter Client UID" field as shared in the documentation
3. Submit the form and verify that the data is inserted into the database using the link shared in the Documentation.

### Verifying Data

- Use MySQL phpmyadmin or another database client to verify the inserted or updated data in the `kobo_data_db` database.

## Database Schema

Based on the data extracted from kobotoolbox, I decided to create a schema with 5 tables that are normalized to the 3rd normalization (surveys, section_a, section_b, section_c and business_status)

![alt text](/assets/image.png)

- **Surveys Table**: Stores general survey information.
- **Section A Table**: Stores section A data, linked to surveys.
- **Section B Table**: Stores section B data, linked to surveys.
- **Section C Table**: Stores section C data, linked to surveys.
- **Business Status Table**: Stores business status data, linked to surveys.

## Error Handling and Logging

- **Webhook**: Errors during data processing are logged in app.log, and a 500 error is returned to the sender.
- **Database Operation**: All Database related logs are captured (database_operations.log). It also ensures that all logs are saved to a file while also allowing for real-time feedback on errors in the console.
- **ETL Script**: Errors during extraction or saving data are logged, and the script exits gracefully.

## Testing

1. **Submit New Data**: Enter a new UID in the form and submit. Verify data insertion in the database.
2. **Update Existing Data**: Use the same UID with updated data. Verify that the corresponding record is updated.
3. **Error Scenarios**: Submit invalid data and ensure appropriate error handling and logging. 

## Assumptions

- The webhook will only receive data from KoboToolbox in the expected format.
- The database schema is flexible enough to handle form structure changes.
- A stable internet connection is assumed for real-time updates.
