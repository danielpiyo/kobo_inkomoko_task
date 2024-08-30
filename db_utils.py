# db_utils.py
from models import Survey, SectionA, SectionB, SectionC, BusinessStatus, Base
from config import Session, engine
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_data_to_db(data):
    session = Session()
    try:
        for record in data:
            # Handling the surveys table
            existing_survey = session.query(Survey).filter_by(uuid=record.get('_uuid')).first()
            if existing_survey:
                # Updating existing survey
                existing_survey.starttime = datetime.fromisoformat(record.get('starttime'))
                existing_survey.endtime = datetime.fromisoformat(record.get('endtime'))
                existing_survey.cd_survey_date = record.get('cd_survey_date')
                existing_survey.meta_instanceID = record.get('meta/instanceID')
                existing_survey.xform_id_string = record.get('_xform_id_string')
                existing_survey.submission_time = datetime.fromisoformat(record.get('_submission_time'))
                existing_survey.status = record.get('_status')
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
           
            # Handling section_a table
            section_a = session.query(SectionA).filter_by(unique_id=record.get('sec_a/unique_id')).first()
            if section_a:
                # Updating existing section_a
                section_a.cd_biz_country_name = record.get('sec_a/cd_biz_country_name')
                section_a.cd_biz_region_name = record.get('sec_a/cd_biz_region_name')
            else:
                # Inserting new section_a
                section_a = SectionA(
                    survey_id=existing_survey.survey_id if existing_survey else survey.survey_id,
                    unique_id=record.get('sec_a/unique_id'),
                    cd_biz_country_name=record.get('sec_a/cd_biz_country_name'),
                    cd_biz_region_name=record.get('sec_a/cd_biz_region_name')
                )
                session.add(section_a)
          
            # Handling section_b table
            section_b = session.query(SectionB).filter_by(survey_id=existing_survey.survey_id if existing_survey else survey.survey_id).first()
            if section_b:
                # Updating existing section_b
                section_b.bda_name = record.get('sec_b/bda_name')
                section_b.cd_cohort = record.get('sec_b/cd_cohort')
                section_b.cd_program = record.get('sec_b/cd_program')
            else:
                # Inserting new section_b
                section_b = SectionB(
                    survey_id=existing_survey.survey_id if existing_survey else survey.survey_id,
                    bda_name=record.get('sec_b/bda_name'),
                    cd_cohort=record.get('sec_b/cd_cohort'),
                    cd_program=record.get('sec_b/cd_program')
                )
                session.add(section_b)
            
            # Handling section_c table
            section_c = session.query(SectionC).filter_by(cd_client_id_manifest=record.get('sec_c/cd_client_id_manifest')).first()
            if section_c:
                # Updating existing section_c
                section_c.cd_client_name = record.get('sec_c/cd_client_name')
                section_c.cd_location = record.get('sec_c/cd_location')
                section_c.cd_clients_phone = record.get('sec_c/cd_clients_phone')
                section_c.cd_clients_phone_smart_feature = record.get('sec_c/cd_clients_phone_smart_feature')
                section_c.cd_gender = record.get('sec_c/cd_gender')
                section_c.cd_age = int(record.get('sec_c/cd_age'))
                section_c.cd_nationality = record.get('sec_c/cd_nationality')
                section_c.cd_strata = record.get('sec_c/cd_strata')
                section_c.cd_disability = record.get('sec_c/cd_disability')
                section_c.cd_education = record.get('sec_c/cd_education')
                section_c.cd_client_status = record.get('sec_c/cd_client_status')
                section_c.cd_sole_income_earner = record.get('sec_c/cd_sole_income_earner')
                section_c.cd_howrespble_pple = int(record.get('sec_c/cd_howrespble_pple'))
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
           

            # Handling business_status table
            business_status = session.query(BusinessStatus).filter_by(survey_id=existing_survey.survey_id if existing_survey else survey.survey_id).first()
            if business_status:
                # Updating existing business_status
                business_status.cd_biz_status = record.get('group_mx5fl16/cd_biz_status')
                business_status.bd_biz_operating = record.get('group_mx5fl16/bd_biz_operating')
            else:
                # Inserting new business_status
                business_status = BusinessStatus(
                    survey_id=existing_survey.survey_id if existing_survey else survey.survey_id,
                    cd_biz_status=record.get('group_mx5fl16/cd_biz_status'),
                    bd_biz_operating=record.get('group_mx5fl16/bd_biz_operating')  # This can be NULL
                )
                session.add(business_status)
           
        session.commit()
        logger.info("Extracted data saved to the database successfully.")
    except Exception as e:
        session.rollback()
        logger.error(f"An error occurred while saving extracted data: {e}")
    finally:
        session.close()

