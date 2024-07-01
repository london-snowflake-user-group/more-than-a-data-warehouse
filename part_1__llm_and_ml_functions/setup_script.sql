/***********************************************************************************************

██████╗ ███████╗ █████╗ ██████╗     ███╗   ███╗███████╗██╗
██╔══██╗██╔════╝██╔══██╗██╔══██╗    ████╗ ████║██╔════╝██║
██████╔╝█████╗  ███████║██║  ██║    ██╔████╔██║█████╗  ██║
██╔══██╗██╔══╝  ██╔══██║██║  ██║    ██║╚██╔╝██║██╔══╝  ╚═╝
██║  ██║███████╗██║  ██║██████╔╝    ██║ ╚═╝ ██║███████╗██╗
╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═════╝     ╚═╝     ╚═╝╚══════╝╚═╝

************************************************************************************************
Copyright(c): 2024 Snowflake Inc. All rights reserved.

This script has been developed to help you understand how to use Snowflake AI & ML features 
within the Snowsight UI and in Streamlit. To use this script follow the readme below

Disclaimer: The code below is provided "as is" and provided with no warranties, support or 
liabilities, use at your own discretion! In no event shall Snowflake or contributors be liable 
for any direct, indirect, incidental, special, exemplary, or consequential damages sustained by 
you or a third party, however caused, arising in any way out of the use of this sample code.

To complete the demo you will need to:
1. Create your Snowflake Trial Account:
- Go to https://signup.snowflake.com/
- Sign up for an ENTERPRISE trial in AWS Oregon.
- Click on the activation link in the email sent to you.
- Set your username and password.

2. Setup the demo environment:
- Create a new SQL Worksheet.
- Copy and paste all the code in this SQL script into the Worksheet.
- Run all the statements below.

***********************************************************************************************/

-- We're not worried about permissions so ACCOUNTADMIN is OK for this, but don't do it in 
-- real life!
use role ACCOUNTADMIN;
use warehouse COMPUTE_WH;



-- An XSMALL will do, also don't need a 10 min auto-suspend which is the default for trials.
alter warehouse COMPUTE_WH set
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
;



-- We need a Snowpark Optimised Warehouse to train the ML model.
create or replace warehouse SNOWPARK_OPTIMIZED_WH
    warehouse_type = 'SNOWPARK-OPTIMIZED'
    warehouse_size = 'MEDIUM'
    max_cluster_count = 1
    min_cluster_count = 1
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    comment = 'Used for memory intensive applications such as training ML models.';
    



-- You shouldn't use ACCOUNTADMIN for creating objects but we're just testing here so it will
-- be fine, we need to make sure ACCOUNTADMIN can use Cortex Functions.
grant database role SNOWFLAKE.CORTEX_USER to role ACCOUNTADMIN;



-- Create DB and Schemas.
create or replace database UCKFIELD_CINEMA;
create or replace schema UCKFIELD_CINEMA.CRM;
create or replace schema UCKFIELD_CINEMA.EPOS;



-- We need some reference data for a number of reasons including using it as an
-- input for our ML models.
use schema UCKFIELD_CINEMA.PUBLIC;



-- Create a reference date table with a date for each ticket combination.
create or replace table UCKFIELD_CINEMA.PUBLIC.DATE_DIM (
    DATE_VALUE date not null,
    YEAR integer not null,
    MONTH integer not null,
    MONTH_NAME varchar not null,
    DAY_OF_MONTH integer not null,
    DAY_OF_WEEK varchar not null,
    DAY_NAME varchar not null,
    WEEK_OF_YEAR integer not null,
    DAY_OF_YEAR integer not null,
    IS_SCHOOL_HOLIDAY_WEEK integer not null,
    TICKET_TYPE varchar not null
) as 
    with CTE_DATE_VALUES as (
        select dateadd( day, seq4(), '2020-01-01') as DATE_VALUE
        from table( generator( rowcount => 1825 ) )
    )
    select 
        DATE_VALUE,
        year( DATE_VALUE ),
        month( DATE_VALUE ),
        monthname( DATE_VALUE ),
        day( DATE_VALUE ),
        dayofweekiso( DATE_VALUE ),
        dayname( DATE_VALUE ),
        weekofyear( DATE_VALUE ),
        dayofyear( DATE_VALUE ),
        case
            when week( DATE_VALUE ) in ( 8, 9, 14, 15, 16, 17, 22, 23, 31,	32,	33,	34,	35,	36, 44,	45, 51,	52,	53 ) then 1
            else 0
        end as IS_SCHOOL_HOLIDAY_WEEK,
        TICKET_TYPES.*,
        
from CTE_DATE_VALUES

    join ( select * from values('Adult'), ('Child'), ('Senior') ) as TICKET_TYPES
;



-- Create a table listing all the LLMs available to use later on so users can choose which LLM to use.
create or replace table UCKFIELD_CINEMA.PUBLIC.LLM (
    NAME varchar,
    CORTEX_FUNCTION varchar
) as 
    select * from values
        ( 'llama3-8b', 'COMPLETE' ),
        ( 'llama3-70b', 'COMPLETE' ),
        ( 'snowflake-arctic', 'COMPLETE' ), -- snowflake-arctic is only available in AWS US West 2 (Oregon) at the moment.
        -- ( 'reka-core', 'COMPLETE' ), -- reka-core is NOT available in AWS US West 2 (Oregon) at the moment.
        ( 'reka-flash', 'COMPLETE' ),
        ( 'mistral-large', 'COMPLETE' ),
        ( 'mixtral-8x7b', 'COMPLETE' ),
        ( 'llama2-70b-chat', 'COMPLETE' ),
        ( 'mistral-7b', 'COMPLETE' ),
        ( 'gemma-7b', 'COMPLETE' )
;



-- Now build the sales data.
use schema UCKFIELD_CINEMA.EPOS;



-- Table to hold cinema reference data.
-- create or replace table CINEMAS (
--     CINEMA_ID number,
--     NAME varchar
-- );



-- insert into CINEMAS values
--   ( 1, 'Uckfield' ),
--   ( 2, 'Paris' )
--   ;



-- Table to hold ticket sales data which is what we'll be forecasting using ML functions.
create or replace table TICKET_SALES (
    CINEMA_ID number,
    TICKET_DATE date,
    TICKET_TYPE varchar,
    QTY int,
    TOTAL_AMOUNT number(10,2)
) as 
select
        1 as CINEMA_ID,
        DATE_VALUE as TICKET_DATE,
        TICKET_TYPE,
        case
            when TICKET_TYPE = 'Adult' then uniform( 1250, 1500, random() )
            when TICKET_TYPE = 'Child' then uniform( 800, 900, random() )
            when TICKET_TYPE = 'Senior' then uniform( 700, 750, random() )
        end
        * 
            case
                when IS_SCHOOL_HOLIDAY_WEEK = 1 then 
                    iff( dayofweekiso( DATE_VALUE ) between 1.0 and 5.0, 
                        decode( 
                            TICKET_TYPE,
                            'Adult', 3.0,
                            'Child', 4.0,
                            'Senior', 1.0
                        ), 
                        decode( 
                            TICKET_TYPE,
                            'Adult', 4.0,
                            'Child', 4.0,
                            'Senior', 0.8
                        )
                    )
                else iff( dayofweekiso( DATE_VALUE ) between 1.0 and 5.0, 
                        decode( 
                            TICKET_TYPE,
                            'Adult', 2.0,
                            'Child', 0.5,
                            'Senior', 2.0
                        ), 
                        decode( 
                            TICKET_TYPE,
                            'Adult', 2.5,
                            'Child', 2.0,
                            'Senior', 1.75
                        )
                    )
            end as TOTAL_AMOUNT,
        round( TOTAL_AMOUNT / 
            decode( 
                TICKET_TYPE,
                'Adult', 10.95,
                'Child', 6.95,
                'Senior', 8.00
            )
        , 0 ) as QTY,
        
    from UCKFIELD_CINEMA.PUBLIC.DATE_DIM

    where DATE_VALUE < current_date()

    order by 2
;



-- Build the reviews table.
use schema UCKFIELD_CINEMA.CRM;



-- Create a table to hold the reviews.
create or replace table UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS (
    REVIEW_DATE timestamp_ntz,
    NAME varchar,
    EMAIL varchar,
    LANG varchar,
    REVIEW varchar,
    REVIEW_EN varchar,
    SENTIMENT float,
    REVIEW_EN_SUMMARY varchar,
    OFFER varchar
);



-- Insert some data.
set EMAIL_ADDRESS = 'your_email@here.com';

-- Insert reviews including a couple of French ones.
insert overwrite into UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS ( REVIEW_DATE, NAME, EMAIL, LANG, REVIEW ) values 
    ( '2024-01-13 09:00:00', 'Dynamo Snare', $EMAIL_ADDRESS, 'en', 'Have been a resident of Uckfield for 30+ years and as such have been a regular patron of the Cinema.  We were due to go to the Lounge tonight, but unfortunately my wife has contracted Covid today.  I rang to see if my tickets could be deferred as it would be irresponsible to attend and was met with a friendly voice, who tried to help. Unfortunately she could not, as we, apparently need to give 24 hours notice to cancel.  I did explain that I wasn''t after a credit, just simply that I could use those tickets on another day.  She put me through to the manager, who although obviously briefed by her colleague just abruptly asked How she could help. Then when a repeated what I said =, she simply just said its company policy and that''s already been explained to you!  So nothing can be done.  I assume I will get a refund for the drinks I''ve ordered ? Or is that company policy to charge for something I haven''t drunk as well as a seat I''ve not sat in?' ),
    
    ( '2024-01-15 09:00:00', 'Chronotrapper', $EMAIL_ADDRESS, 'en', 'First time visiting this cinema, and I was really impressed! The seats are super comfy and I loved being able to order food and drinks to my seat. The sound was set at a comfortable level too, I usually find the sound much too loud at other cinemas. Looking forward to going back!' ),
    
    ( '2024-01-13 09:00:00', 'Atomic Ember', $EMAIL_ADDRESS,  'en','My wife and I randomly found ourselves staying in Uckfield at the weekend (we had to be somewhere local in the morning so stayed over) and we were looking for something to do in the evening. We found this amazing place only a few minutes away from where we were staying. We booked dinner at the restaurant - lovely film inspired decor, a friendly welcome and great service, and really good food, reasonably priced. We then popped over the road to the cinema to watch the new Mission Impossible in their ''lounge'' cinema which was a fantastic experience. Reclining leather armchairs, waitress service before the film for all your food and drink needs - was great to enjoy a pint with the film in such comfort. The only issue is being so comfortable you could end up having a snooze!! If you''re ever passing through I highly recommend stopping here, it''s a great night out. Thanks so much!' ),
    
    ( '2024-01-21 09:00:00', 'Insect Catch', $EMAIL_ADDRESS, 'en', 'If you want just a fantastic cinema experience and a great place to eat beforehand then look no further! The restaurant menu is fantastic. I have celiac disease and their GF menu is brilliant with a large choice. The cinema is just wonderful!! The cinema is clearly loved by those who own it and work there. Forget the normal chain cinemas , Uckfield Picture House offers a truly wonderful and memorable experience. Table service to your seat. Have a beer and food at your seat and enjoy the movie!' ),
    
    ( '2024-01-29 09:00:00', 'Kick Smasher', $EMAIL_ADDRESS,  'en','A lovely little cinema with an easy free car park nearby, which is a plus. Comfy reclining seats, food and drinks brought to you by friendly staff, latest films, what more would you want?!' ),
    
    ( '2024-03-04 09:00:00', 'Mental Vine', $EMAIL_ADDRESS, 'en', 'The Lounge screen is very cool. Comfy large electric reclining seats with raised foot rest (don''t fall asleep!). Get your own mini table with a menu of hot and cold snacks. These aren''t your standards menu offerings you see at cinemas, and the food is tasty. Can order alcohol too. I can recommend the nachos. Someone comes and takes your order from your seat, and brings it to you. Would be better if they had the option to order on your phone from a qr code. Can order in advance though if you book your ticket online.' ),
        
    ( '2024-03-24 09:00:00', 'Steelo', $EMAIL_ADDRESS, 'en', 'We live equi distance between Uckfield & Tunbridge Wells, so have the choice of a large multiplex or the Uckfield picture house. We always pick the lounge at the picture house. Lovely experience. Comfy reclining seats. Table service for drinks and food and great quality viewing experience. We then often pop over to their restaurant over the road which is great quality too.' ),
    
    ( '2024-04-27 09:00:00', 'Tree Raven', $EMAIL_ADDRESS, 'en', 'We were a group of 16 but despite pre-ordering  and paying a deposit we waited nearly two hours for our main courses.  When food came it came all together, hot and very good which was a plus.  Very crowded.' ),
    
    ( '2024-05-02 09:00:00', 'Virus Woman', $EMAIL_ADDRESS, 'en', 'Nice cinema but can be a bit too loud sometimes. Prefer screen 1 to screen 3 so if screen 3 stay away from the farthest seats as only one aisle. Otherwise worth patronising.' ),
    
    ( '2024-05-10 09:00:00', 'Wind Tide', $EMAIL_ADDRESS, 'en', 'I like the Picture House, it''s a lovely cinema and the recent refurbishment is excellent. I''m only giving three stars however as in a recent trip we were treated quite rudely by the management, unnecessarily.' ),
    
    ( '2024-06-13 09:00:00', 'Xavi A', $EMAIL_ADDRESS, 'fr', 'Vraiment super petit cinéma à Uckfield. Ils ont rénové les salles avec moustiquaire afin que même la rangée 1 offre une vue très confortable. Vous pouvez commander de la nourriture et des boissons à apporter à votre place dans l''écran 1 et dans le salon, qui dispose également de sièges inclinables très confortables. Hautement recommandé.' ),
    
    ( '2024-06-19 09:00:00', 'Dancing Cat', $EMAIL_ADDRESS, 'fr', 'Un charmant petit lieu avec un décor intéressant lié au cinéma et servant de la bonne nourriture à des prix raisonnables. Cela vaut vraiment le détour et si vous voulez aussi voir un film, le cinéma est juste de l''autre côté de la rue.' )
;



-- We often end up with lots of tables and views that need to cleared up. This SP
-- will allow you to drop lots of tables or views at once.
create or replace procedure PUBLIC.DROP_TABLES_OR_VIEWS(
    table_type varchar,
    table_database varchar, 
    table_schema varchar, 
    table_pattern varchar
)
returns varchar 
language javascript
execute as caller
as
$$
var table_type = TABLE_TYPE
var table_database = TABLE_DATABASE
var table_schema = TABLE_SCHEMA
var table_pattern = TABLE_PATTERN
var result = "";

var sql_command = `select TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME from ` + table_database + `."INFORMATION_SCHEMA"."TABLES" where TABLE_TYPE = '` + table_type + `'and TABLE_SCHEMA = '` + table_schema + `'  and TABLE_NAME ilike '%` + table_pattern+ `%'`;
var stmt = snowflake.createStatement( {sqlText: sql_command} );
var resultSet = stmt.execute();
while (resultSet.next()){

try { 
    if (table_type == 'BASE TABLE') {  
        var table_name = resultSet.getColumnValue(1);
        var sql_command = `drop table `  + table_name + `;`;    
        snowflake.execute ({sqlText: sql_command});
        result = result + "dropped: " + table_name + "\n"
    }

    else {
        var table_name = resultSet.getColumnValue(1);
        var sql_command = `drop ` + table_type +` ` + table_name + `;`;
        snowflake.execute ({sqlText: sql_command});
        result = result + "dropped: " + table_name + "\n"}
    }
    catch (err)  {
        result =  "Error: " + err.message;
    }
}

return result; 
$$;


-- call drop_tables_or_views ( 'VIEW', 'UCKFIELD_CINEMA', 'EPOS', 'TICKET_SALES');

