import logging
from datetime import datetime
from models import Survey, SectionA, SectionB, SectionC, BusinessStatus, Base
from config import Session, engine

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create file handler which logs even debug messages
fh = logging.FileHandler('database_operations.log')
fh.setLevel(logging.INFO)

# Create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# Create a formatter and set it for the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def save_data_to_db(data):
    session = Session()
    try:
        for record in data:
            # Handling the surveys table
            existing_survey = session.query(Survey).filter_by(uuid=record.get('_uuid')).first()
            if existing_survey:
                # Check if there's a change in the survey record
                changes = []
                if existing_survey.starttime != datetime.fromisoformat(record.get('starttime')):
                    changes.append(f"starttime: {existing_survey.starttime} -> {record.get('starttime')}")
                    existing_survey.starttime = datetime.fromisoformat(record.get('starttime'))
                if existing_survey.endtime != datetime.fromisoformat(record.get('endtime')):
                    changes.append(f"endtime: {existing_survey.endtime} -> {record.get('endtime')}")
                    existing_survey.endtime = datetime.fromisoformat(record.get('endtime'))
                if existing_survey.cd_survey_date != record.get('cd_survey_date'):
                    changes.append(f"cd_survey_date: {existing_survey.cd_survey_date} -> {record.get('cd_survey_date')}")
                    existing_survey.cd_survey_date = record.get('cd_survey_date')
                if existing_survey.meta_instanceID != record.get('meta/instanceID'):
                    changes.append(f"meta_instanceID: {existing_survey.meta_instanceID} -> {record.get('meta/instanceID')}")
                    existing_survey.meta_instanceID = record.get('meta/instanceID')
                if existing_survey.xform_id_string != record.get('_xform_id_string'):
                    changes.append(f"xform_id_string: {existing_survey.xform_id_string} -> {record.get('_xform_id_string')}")
                    existing_survey.xform_id_string = record.get('_xform_id_string')
                if existing_survey.submission_time != datetime.fromisoformat(record.get('_submission_time')):
                    changes.append(f"submission_time: {existing_survey.submission_time} -> {record.get('_submission_time')}")
                    existing_survey.submission_time = datetime.fromisoformat(record.get('_submission_time'))
                if existing_survey.status != record.get('_status'):
                    changes.append(f"status: {existing_survey.status} -> {record.get('_status')}")
                    existing_survey.status = record.get('_status')

                if changes:
                    logger.info(f"Updated Survey {existing_survey.uuid}: " + ", ".join(changes))
            else:
                # Inserting new survey
                survey = Survey(
                    uuid=record.get('_uuid'),
                    starttime=datetime.fromisoformat(record.get('starttime')),
                    endtime=datetime.fromisoformat(record.get('endtime')),
                    cd_survey_date=record.get('cd_survey_date'),
                    meta_instanceID=record.get('meta/instanceID'),
                    xform_id_string=record.get('_xform_id_string'),
                    submission_time=datetime.fromisoformat(record.get('_submission_time')),
                    status=record.get('_status')
                )
                session.add(survey)
                session.flush()  
                logger.info(f"Inserted new Survey {survey.uuid}")

            # Handling section_a table
            section_a = session.query(SectionA).filter_by(unique_id=record.get('sec_a/unique_id')).first()
            if section_a:
                # Check if there's a change in the section_a record
                changes = []
                if section_a.cd_biz_country_name != record.get('sec_a/cd_biz_country_name'):
                    changes.append(f"cd_biz_country_name: {section_a.cd_biz_country_name} -> {record.get('sec_a/cd_biz_country_name')}")
                    section_a.cd_biz_country_name = record.get('sec_a/cd_biz_country_name')
                if section_a.cd_biz_region_name != record.get('sec_a/cd_biz_region_name'):
                    changes.append(f"cd_biz_region_name: {section_a.cd_biz_region_name} -> {record.get('sec_a/cd_biz_region_name')}")
                    section_a.cd_biz_region_name = record.get('sec_a/cd_biz_region_name')

                if changes:
                    logger.info(f"Updated SectionA {section_a.unique_id}: " + ", ".join(changes))
            else:
                # Inserting new section_a
                section_a = SectionA(
                    survey_id=existing_survey.survey_id if existing_survey else survey.survey_id,
                    unique_id=record.get('sec_a/unique_id'),
                    cd_biz_country_name=record.get('sec_a/cd_biz_country_name'),
                    cd_biz_region_name=record.get('sec_a/cd_biz_region_name')
                )
                session.add(section_a)
                logger.info(f"Inserted new SectionA {section_a.unique_id}")
          
            # Handling section_b table
            section_b = session.query(SectionB).filter_by(survey_id=existing_survey.survey_id if existing_survey else survey.survey_id).first()
            if section_b:
                # Check if there's a change in the section_b record
                changes = []
                if section_b.bda_name != record.get('sec_b/bda_name'):
                    changes.append(f"bda_name: {section_b.bda_name} -> {record.get('sec_b/bda_name')}")
                    section_b.bda_name = record.get('sec_b/bda_name')
                if section_b.cd_cohort != record.get('sec_b/cd_cohort'):
                    changes.append(f"cd_cohort: {section_b.cd_cohort} -> {record.get('sec_b/cd_cohort')}")
                    section_b.cd_cohort = record.get('sec_b/cd_cohort')
                if section_b.cd_program != record.get('sec_b/cd_program'):
                    changes.append(f"cd_program: {section_b.cd_program} -> {record.get('sec_b/cd_program')}")
                    section_b.cd_program = record.get('sec_b/cd_program')

                if changes:
                    logger.info(f"Updated SectionB for survey_id {section_b.survey_id}: " + ", ".join(changes))
            else:
                # Inserting new section_b
                section_b = SectionB(
                    survey_id=existing_survey.survey_id if existing_survey else survey.survey_id,
                    bda_name=record.get('sec_b/bda_name'),
                    cd_cohort=record.get('sec_b/cd_cohort'),
                    cd_program=record.get('sec_b/cd_program')
                )
                session.add(section_b)
                logger.info(f"Inserted new SectionB for survey_id {section_b.survey_id}")

            # Handling section_c table
            section_c = session.query(SectionC).filter_by(cd_client_id_manifest=record.get('sec_c/cd_client_id_manifest')).first()
            if section_c:
                # Check if there's a change in the section_c record
                changes = []
                if section_c.cd_client_name != record.get('sec_c/cd_client_name'):
                    changes.append(f"cd_client_name: {section_c.cd_client_name} -> {record.get('sec_c/cd_client_name')}")
                    section_c.cd_client_name = record.get('sec_c/cd_client_name')
                if section_c.cd_location != record.get('sec_c/cd_location'):
                    changes.append(f"cd_location: {section_c.cd_location} -> {record.get('sec_c/cd_location')}")
                    section_c.cd_location = record.get('sec_c/cd_location')
                if section_c.cd_clients_phone != record.get('sec_c/cd_clients_phone'):
                    changes.append(f"cd_clients_phone: {section_c.cd_clients_phone} -> {record.get('sec_c/cd_clients_phone')}")
                    section_c.cd_clients_phone = record.get('sec_c/cd_clients_phone')
                if section_c.cd_clients_phone_smart_feature != record.get('sec_c/cd_clients_phone_smart_feature'):
                    changes.append(f"cd_clients_phone_smart_feature: {section_c.cd_clients_phone_smart_feature} -> {record.get('sec_c/cd_clients_phone_smart_feature')}")
                    section_c.cd_clients_phone_smart_feature = record.get('sec_c/cd_clients_phone_smart_feature')
                if section_c.cd_gender != record.get('sec_c/cd_gender'):
                    changes.append(f"cd_gender: {section_c.cd_gender} -> {record.get('sec_c/cd_gender')}")
                    section_c.cd_gender = record.get('sec_c/cd_gender')
                if section_c.cd_age != int(record.get('sec_c/cd_age')):
                    changes.append(f"cd_age: {section_c.cd_age} -> {record.get('sec_c/cd_age')}")
                    section_c.cd_age = int(record.get('sec_c/cd_age'))
                if section_c.cd_nationality != record.get('sec_c/cd_nationality'):
                    changes.append(f"cd_nationality: {section_c.cd_nationality} -> {record.get('sec_c/cd_nationality')}")
                    section_c.cd_nationality = record.get('sec_c/cd_nationality')
                if section_c.cd_strata != record.get('sec_c/cd_strata'):
                    changes.append(f"cd_strata: {section_c.cd_strata} -> {record.get('sec_c/cd_strata')}")
                    section_c.cd_strata = record.get('sec_c/cd_strata')
                if section_c.cd_disability != record.get('sec_c/cd_disability'):
                    changes.append(f"cd_disability: {section_c.cd_disability} -> {record.get('sec_c/cd_disability')}")
                    section_c.cd_disability = record.get('sec_c/cd_disability')
                if section_c.cd_education != record.get('sec_c/cd_education'):
                    changes.append(f"cd_education: {section_c.cd_education} -> {record.get('sec_c/cd_education')}")
                    section_c.cd_education = record.get('sec_c/cd_education')
                if section_c.cd_client_status != record.get('sec_c/cd_client_status'):
                    changes.append(f"cd_client_status: {section_c.cd_client_status} -> {record.get('sec_c/cd_client_status')}")
                    section_c.cd_client_status = record.get('sec_c/cd_client_status')
                if section_c.cd_sole_income_earner != record.get('sec_c/cd_sole_income_earner'):
                    changes.append(f"cd_sole_income_earner: {section_c.cd_sole_income_earner} -> {record.get('sec_c/cd_sole_income_earner')}")
                    section_c.cd_sole_income_earner = record.get('sec_c/cd_sole_income_earner')
                if section_c.cd_howrespble_pple != int(record.get('sec_c/cd_howrespble_pple')):
                    changes.append(f"cd_howrespble_pple: {section_c.cd_howrespble_pple} -> {record.get('sec_c/cd_howrespble_pple')}")
                    section_c.cd_howrespble_pple = int(record.get('sec_c/cd_howrespble_pple'))

                if changes:
                    logger.info(f"Updated SectionC {section_c.cd_client_id_manifest}: " + ", ".join(changes))
            else:
                # Inserting new section_c
                section_c = SectionC(
                    survey_id=existing_survey.survey_id if existing_survey else survey.survey_id,
                    cd_client_name=record.get('sec_c/cd_client_name'),
                    cd_client_id_manifest=record.get('sec_c/cd_client_id_manifest'),
                    cd_location=record.get('sec_c/cd_location'),
                    cd_clients_phone=record.get('sec_c/cd_clients_phone'),
                    cd_clients_phone_smart_feature=record.get('sec_c/cd_clients_phone_smart_feature'),
                    cd_gender=record.get('sec_c/cd_gender'),
                    cd_age=int(record.get('sec_c/cd_age')),
                    cd_nationality=record.get('sec_c/cd_nationality'),
                    cd_strata=record.get('sec_c/cd_strata'),
                    cd_disability=record.get('sec_c/cd_disability'),
                    cd_education=record.get('sec_c/cd_education'),
                    cd_client_status=record.get('sec_c/cd_client_status'),
                    cd_sole_income_earner=record.get('sec_c/cd_sole_income_earner'),
                    cd_howrespble_pple=int(record.get('sec_c/cd_howrespble_pple'))
                )
                session.add(section_c)
                logger.info(f"Inserted new SectionC {section_c.cd_client_id_manifest}")

            # Handling business_status table
            business_status = session.query(BusinessStatus).filter_by(survey_id=existing_survey.survey_id if existing_survey else survey.survey_id).first()
            if business_status:
                # Check if there's a change in the business_status record
                changes = []
                if business_status.cd_biz_status != record.get('group_mx5fl16/cd_biz_status'):
                    changes.append(f"cd_biz_status: {business_status.cd_biz_status} -> {record.get('group_mx5fl16/cd_biz_status')}")
                    business_status.cd_biz_status = record.get('group_mx5fl16/cd_biz_status')
                if business_status.bd_biz_operating != record.get('group_mx5fl16/bd_biz_operating'):
                    changes.append(f"bd_biz_operating: {business_status.bd_biz_operating} -> {record.get('group_mx5fl16/bd_biz_operating')}")
                    business_status.bd_biz_operating = record.get('group_mx5fl16/bd_biz_operating')

                if changes:
                    logger.info(f"Updated BusinessStatus for survey_id {business_status.survey_id}: " + ", ".join(changes))
            else:
                # Inserting new business_status
                business_status = BusinessStatus(
                    survey_id=existing_survey.survey_id if existing_survey else survey.survey_id,
                    cd_biz_status=record.get('group_mx5fl16/cd_biz_status'),
                    bd_biz_operating=record.get('group_mx5fl16/bd_biz_operating')  # This can be NULL
                )
                session.add(business_status)
                logger.info(f"Inserted new BusinessStatus for survey_id {business_status.survey_id}")

        session.commit()
        logger.info("Extracted data saved to the database successfully.")
    except Exception as e:
        logger.error(f"An error occurred while saving extracted data: {e}") 
        session.rollback()
    finally:
        session.close()
