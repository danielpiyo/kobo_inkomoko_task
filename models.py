from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Survey(Base):
    __tablename__ = 'surveys'
    survey_id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String(255), unique=True, nullable=False)
    starttime = Column(DateTime, nullable=False)
    endtime = Column(DateTime, nullable=False)
    cd_survey_date = Column(String(50), nullable=False)
    meta_instanceID = Column(String(255), nullable=False)
    xform_id_string = Column(String(255), nullable=False)
    submission_time = Column(DateTime, nullable=False)
    status = Column(String(50))

    # Relationships
    section_a = relationship("SectionA", back_populates="survey", cascade="all, delete-orphan")
    section_b = relationship("SectionB", back_populates="survey", cascade="all, delete-orphan")
    section_c = relationship("SectionC", back_populates="survey", cascade="all, delete-orphan")
    business_status = relationship("BusinessStatus", back_populates="survey", cascade="all, delete-orphan")

class SectionA(Base):
    __tablename__ = 'section_a'
    id = Column(Integer, primary_key=True, autoincrement=True)
    survey_id = Column(Integer, ForeignKey('surveys.survey_id'), nullable=False)
    unique_id = Column(String(255), nullable=False)
    cd_biz_country_name = Column(String(255), nullable=False)
    cd_biz_region_name = Column(String(255), nullable=False)

    # Relationship
    survey = relationship("Survey", back_populates="section_a")

class SectionB(Base):
    __tablename__ = 'section_b'
    id = Column(Integer, primary_key=True, autoincrement=True)
    survey_id = Column(Integer, ForeignKey('surveys.survey_id'), nullable=False)
    bda_name = Column(String(255), nullable=False)
    cd_cohort = Column(String(255), nullable=False)
    cd_program = Column(String(255), nullable=False)

    # Relationship
    survey = relationship("Survey", back_populates="section_b")

class SectionC(Base):
    __tablename__ = 'section_c'
    id = Column(Integer, primary_key=True, autoincrement=True)
    survey_id = Column(Integer, ForeignKey('surveys.survey_id'), nullable=False)
    cd_client_name = Column(String(255), nullable=False)
    cd_client_id_manifest = Column(String(255), nullable=False)
    cd_location = Column(String(255), nullable=False)
    cd_clients_phone = Column(String(255), nullable=False)
    cd_clients_phone_smart_feature = Column(String(255), nullable=False)
    cd_gender = Column(String(50), nullable=False)
    cd_age = Column(Integer, nullable=False)
    cd_nationality = Column(String(255), nullable=False)
    cd_strata = Column(String(255), nullable=False)
    cd_disability = Column(String(50), nullable=False)
    cd_education = Column(String(255), nullable=False)
    cd_client_status = Column(String(255), nullable=False)
    cd_sole_income_earner = Column(String(50), nullable=False)
    cd_howrespble_pple = Column(Integer, nullable=False)

    # Relationship
    survey = relationship("Survey", back_populates="section_c")

class BusinessStatus(Base):
    __tablename__ = 'business_status'
    id = Column(Integer, primary_key=True, autoincrement=True)
    survey_id = Column(Integer, ForeignKey('surveys.survey_id'), nullable=False)
    cd_biz_status = Column(String(255), nullable=False)
    bd_biz_operating = Column(String(50), nullable=False)

    # Relationship
    survey = relationship("Survey", back_populates="business_status")
