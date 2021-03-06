DROP TABLE IF EXISTS agreement;

CREATE TABLE IF NOT EXISTS agreement (
       "schema_name" VARCHAR(50) NOT NULL,
       agreement_id INTEGER NOT NULL,
       created TIMESTAMP WITH TIME ZONE,
       modified TIMESTAMP WITH TIME ZONE,
       partner_id INTEGER,
       partner_partner_type VARCHAR(50),
       partner_name VARCHAR(255),
       country_programme_id INTEGER,
       agreement_type VARCHAR(10),
       agreement_number VARCHAR(45),
       attached_agreement VARCHAR(255),
       "start" DATE,
       "end" DATE,
       signed_by_id INTEGER,
       signed_by_unicef_date DATE,
       signed_by_first_name VARCHAR(30),
       signed_by_last_name VARCHAR(30),
       signed_by_email VARCHAR(254),
       signed_by_partner_date DATE,
       partner_manager_id INTEGER,
       partner_manager_title VARCHAR(64),
       partner_manager_first_name VARCHAR(64),
       partner_manager_last_name VARCHAR(64),
       partner_manager_email VARCHAR(128),
       partner_manager_phone VARCHAR(64),
       status VARCHAR(32)
)
