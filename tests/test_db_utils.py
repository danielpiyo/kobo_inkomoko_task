import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Survey, SectionA, SectionB, SectionC, BusinessStatus
from db_utils import save_data_to_db
from datetime import datetime

# Set up the test engine using the test MySQL database
@pytest.fixture(scope='module')
def test_engine():
    from config import engine  # Use the engine from the config
    Base.metadata.create_all(engine)  # Create all tables in the test database
    return engine

@pytest.fixture(scope='module')
def test_session(test_engine):
    Session = sessionmaker(bind=test_engine)
    session = Session()
    yield session
    session.close()

from datetime import datetime

# Normalize datetime objects by removing microseconds and tzinfo
def normalize_datetime(dt):
    return dt.replace(tzinfo=None, microsecond=0)

def test_insert_new_data(test_session):
    # Clear tables before running the test
    test_session.query(BusinessStatus).delete()
    test_session.query(SectionC).delete()
    test_session.query(SectionB).delete()
    test_session.query(SectionA).delete()
    test_session.query(Survey).delete()
    test_session.commit()

    # Sample data to insert
    sample_data = [
        {
            "_id": 375105515,
            "formhub/uuid": "Opiyo7eb959ada4c485b8334ee761ab1e4a7",
            "starttime": "2024-08-24T09:44:06.712+02:00",
            "endtime": "2024-08-24T09:44:39.156+02:00",
            "cd_survey_date": "2024-08-24",
            "sec_a/unique_id": "SS01406240716114357",
            "sec_a/cd_biz_country_name": "South Sudan",
            "sec_a/cd_biz_region_name": "Juba",
            "sec_b/bda_name": "Mussie Measho",
            "sec_b/cd_cohort": "Cohort 2",
            "sec_b/cd_program": "Livelihood",
            "sec_c/cd_client_name": "Daniel Opiyo",
            "sec_c/cd_client_id_manifest": "106-19-00759",
            "sec_c/cd_location": "Juba urban refugees",
            "sec_c/cd_clients_phone": "921168696",
            "sec_c/cd_clients_phone_smart_feature": "Smart phone",
            "sec_c/cd_gender": "Female",
            "sec_c/cd_age": "28",
            "sec_c/cd_nationality": "Burundian",
            "sec_c/cd_strata": "Urban Based Refugee",
            "sec_c/cd_disability": "No",
            "sec_c/cd_education": "Attended primary school",
            "sec_c/cd_client_status": "New clients",
            "sec_c/cd_sole_income_earner": "Yes",
            "sec_c/cd_howrespble_pple": "3",
            "group_mx5fl16/cd_biz_status": "Existing Business",
            "group_mx5fl16/bd_biz_operating": "yes",
            "__version__": "vBfco72yRxvHQun3cF8HPK",
            "meta/instanceID": "uuid:5c59e249-b88e-4742-abb6-942f79627cb6",
            "_xform_id_string": "aW9w8jHjn4Cj8SSQ5VcojK",
            "_uuid": "5c59e249-b88e-4742-abb6-942f79627cb6",
            "_attachments": [],
            "_status": "submitted_via_web",
            "_geolocation": [],
            "_submission_time": "2024-08-24T07:45:34",
            "_tags": [],
            "_notes": [],
            "_validation_status": {},
            "_submitted_by": "Daniel Opiyo"
        }
    ]

    # Call the save_data_to_db function
    save_data_to_db(sample_data)

    # Query the database to check if data was inserted
    survey = test_session.query(Survey).filter_by(uuid="5c59e249-b88e-4742-abb6-942f79627cb6").first()
    assert survey is not None, "Survey should be found in the database"

    # Normalize datetime objects for comparison
    expected_starttime = normalize_datetime(datetime.fromisoformat("2024-08-24T09:44:06.712+02:00"))
    actual_starttime = normalize_datetime(survey.starttime)

    assert actual_starttime == expected_starttime, f"Expected starttime {expected_starttime}, but got {actual_starttime}"

def test_update_existing_data(test_session):
    # Sample data to update
    updated_data = [
        {
            "_id": 375105515,
            "formhub/uuid": "Opiyo7eb959ada4c485b8334ee761ab1e4a7",
            "starttime": "2024-08-24T10:00:00.000+02:00",
            "endtime": "2024-08-24T10:30:00.000+02:00",
            "cd_survey_date": "2024-08-24",
            "sec_a/unique_id": "SS01406240716114357",
            "sec_a/cd_biz_country_name": "South Sudan",
            "sec_a/cd_biz_region_name": "Juba",
            "sec_b/bda_name": "Mussie Measho",
            "sec_b/cd_cohort": "Cohort 2",
            "sec_b/cd_program": "Livelihood",
            "sec_c/cd_client_name": "Daniel Opiyo",
            "sec_c/cd_client_id_manifest": "106-19-00759",
            "sec_c/cd_location": "Juba urban refugees",
            "sec_c/cd_clients_phone": "921168696",
            "sec_c/cd_clients_phone_smart_feature": "Smart phone",
            "sec_c/cd_gender": "Female",
            "sec_c/cd_age": "28",
            "sec_c/cd_nationality": "Burundian",
            "sec_c/cd_strata": "Urban Based Refugee",
            "sec_c/cd_disability": "No",
            "sec_c/cd_education": "Attended primary school",
            "sec_c/cd_client_status": "New clients",
            "sec_c/cd_sole_income_earner": "Yes",
            "sec_c/cd_howrespble_pple": "3",
            "group_mx5fl16/cd_biz_status": "Existing Business",
            "group_mx5fl16/bd_biz_operating": "yes",
            "__version__": "vBfco72yRxvHQun3cF8HPK",
            "meta/instanceID": "uuid:5c59e249-b88e-4742-abb6-942f79627cb6",
            "_xform_id_string": "aW9w8jHjn4Cj8SSQ5VcojK",
            "_uuid": "5c59e249-b88e-4742-abb6-942f79627cb6",
            "_attachments": [],
            "_status": "submitted_via_web",
            "_geolocation": [],
            "_submission_time": "2024-08-24T07:45:34",
            "_tags": [],
            "_notes": [],
            "_validation_status": {},
            "_submitted_by": "Daniel Opiyo"
        }
    ]

    # Call the save_data_to_db function to update the existing record
    save_data_to_db(updated_data)

    # Query the database to check if data was updated
    survey = test_session.query(Survey).filter_by(uuid="5c59e249-b88e-4742-abb6-942f79627cb6").first()
    assert survey is not None, "Survey should be found in the database"

def test_avoid_duplicate_records(test_session):
    # Attempt to insert the same data again
    duplicate_data = [
        {
            "_id": 375105515,
            "formhub/uuid": "Opiyo7eb959ada4c485b8334ee761ab1e4a7",
            "starttime": "2024-08-24T09:44:06.712+02:00",
            "endtime": "2024-08-24T09:44:39.156+02:00",
            "cd_survey_date": "2024-08-24",
            "sec_a/unique_id": "SS01406240716114357",
            "sec_a/cd_biz_country_name": "South Sudan",
            "sec_a/cd_biz_region_name": "Juba",
            "sec_b/bda_name": "Mussie Measho",
            "sec_b/cd_cohort": "Cohort 2",
            "sec_b/cd_program": "Livelihood",
            "sec_c/cd_client_name": "Daniel Opiyo",
            "sec_c/cd_client_id_manifest": "106-19-00759",
            "sec_c/cd_location": "Juba urban refugees",
            "sec_c/cd_clients_phone": "921168696",
            "sec_c/cd_clients_phone_smart_feature": "Smart phone",
            "sec_c/cd_gender": "Female",
            "sec_c/cd_age": "28",
            "sec_c/cd_nationality": "Burundian",
            "sec_c/cd_strata": "Urban Based Refugee",
            "sec_c/cd_disability": "No",
            "sec_c/cd_education": "Attended primary school",
            "sec_c/cd_client_status": "New clients",
            "sec_c/cd_sole_income_earner": "Yes",
            "sec_c/cd_howrespble_pple": "3",
            "group_mx5fl16/cd_biz_status": "Existing Business",
            "group_mx5fl16/bd_biz_operating": "yes",
            "__version__": "vBfco72yRxvHQun3cF8HPK",
            "meta/instanceID": "uuid:5c59e249-b88e-4742-abb6-942f79627cb6",
            "_xform_id_string": "aW9w8jHjn4Cj8SSQ5VcojK",
            "_uuid": "5c59e249-b88e-4742-abb6-942f79627cb6",
            "_attachments": [],
            "_status": "submitted_via_web",
            "_geolocation": [],
            "_submission_time": "2024-08-24T07:45:34",
            "_tags": [],
            "_notes": [],
            "_validation_status": {},
            "_submitted_by": "Daniel Opiyo"
        }
    ]
    
    # Calling the save_data_to_db function to insert duplicate data
    save_data_to_db(duplicate_data)

    # Query the database to check that only one record exists
    survey_count = test_session.query(Survey).filter_by(uuid="5c59e249-b88e-4742-abb6-942f79627cb6").count()
    assert survey_count == 1, "There should be exactly one survey record in the database"
