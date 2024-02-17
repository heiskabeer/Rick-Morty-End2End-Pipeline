create or replace database rm;

create or replace table rickmorty(
id int,
name varchar(50),
image varchar(50),
specie varchar(50),
gender char(10),
origin varchar(50),
status char(10),
no_of_episodes_appeared int
);

create or replace stage s3_stage 
url='s3://transformed-rick-morty' 
credentials=
(AWS_KEY_ID='' 
AWS_SECRET_KEY='');

CREATE OR REPLACE PIPE rick_pipe
AUTO_INGEST=TRUE
AS
COPY INTO rickmorty
FROM @s3_stage
FILE_FORMAT = (TYPE = 'CSV' skip_header = 1);

select COUNT(*) from rickmorty;

SELECT * FROM RICKMORTY;
