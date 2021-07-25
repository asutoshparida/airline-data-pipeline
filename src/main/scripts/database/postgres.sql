create table if not exists pipeline_run_history(
ID SERIAL PRIMARY KEY,
ETL_NAME VARCHAR(60) NOT NULL,
SKIP_EXECUTION CHAR(1) NOT NULL,
FULL_LOAD CHAR(1) NOT NULL,
IS_ACTIVE CHAR(1) NOT NULL,
run_date timestamp,
filter_col1_name VARCHAR(100),
filter_col1_value  VARCHAR(1000),
filter_col2_name VARCHAR(100),
filter_col2_value VARCHAR(1000)
);

create table if not exists exception_log(
ID SERIAL PRIMARY KEY,
ETL_NAME CHAR(60) NOT NULL,
run_id SERIAL,
date_time timestamp,
exception_message text
);

INSERT INTO pipeline_run_history (ETL_NAME , SKIP_EXECUTION, FULL_LOAD, IS_ACTIVE, run_date, filter_col1_name)
VALUES ('airline-data', 'N', 'Y', 'Y', current_timestamp, 'EVENT_DATE');