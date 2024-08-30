from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# Database configuration
DATABASE_URI = os.getenv('DB_URL')

# DATABASE_URI = 'mysql+mysqlconnector://root@localhost/kobo_data_db'

# KoboToolbox API configuration
KOBO_API_URL = os.getenv('KOBO_URL')
KOBO_API_HEADERS = {
    'Authorization': os.getenv('KOBO_HEADER_TOKEN'),
    'Cookie': 'django_language=en'
}

# Setup the MySQL database engine 
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
