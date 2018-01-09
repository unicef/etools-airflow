DROP TABLE IF EXISTS partner_organization;

CREATE TABLE IF NOT EXISTS partner_organization (
       "schema_name" VARCHAR(50) NOT NULL,
       partner_organization_id INTEGER NOT NULL,
       partner_type VARCHAR(50),
       "name" VARCHAR(255),
       short_name VARCHAR(50),
       description VARCHAR(255),
       shared_with VARCHAR(255),
       shared_partner VARCHAR(50),
       street_address VARCHAR(500),
       city VARCHAR(32),
       postal_code VARCHAR(32),
       country VARCHAR(32),
       address VARCHAR(255),
       email VARCHAR(255),
       phone_number VARCHAR(32),
       vendor_number VARCHAR(30),
       alternate_id INTEGER,
       alternate_name VARCHAR(255),
       rating VARCHAR(50),
       type_of_assessment VARCHAR(50),
       last_assessment_date DATE,
       core_values_assessment_date DATE,
       core_values_assessment VARCHAR(1024),
       cso_type VARCHAR(50),
       vision_synced BOOLEAN,
       blocked BOOLEAN,
       hidden BOOLEAN,
       deleted_flag BOOLEAN,
       total_ct_cp NUMERIC(12, 2),
       total_ct_cy NUMERIC(12, 2),
       hact_values VARCHAR(500)
)
