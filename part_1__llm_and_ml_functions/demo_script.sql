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
1. Setup a new Snowflake Account and prepare the demo environment as explained in the 
2024-07 LSUG - Setup Script.sql.

2. Create a new SQL Worksheet and copy and paste all the code in this SQL script into it.



-- FURTHER READING:
-- https://quickstarts.snowflake.com/guide/getting-started-with-snowflake-cortex-ml-forecasting-and-classification/index.html

***********************************************************************************************/



-- EXPLORATORY DATA ANALYSIS
-- ======================================================================================
-- Before building our model, let's first visualize our data to get a feel for what it 
-- looks like and get a sense of what variables we will be working with.

use schema UCKFIELD_CINEMA.EPOS;



-- Create a new view and add some simple date based features our ML model can use later.
create or replace view TICKET_SALES_ENRICHED as 
    select 
        TS.* exclude ( TICKET_TYPE, TICKET_DATE ),
        DD.*,
        
    from TICKET_SALES as TS

        right join PUBLIC.DATE_DIM as DD
            on TS.TICKET_DATE = DD.DATE_VALUE
            and TS.TICKET_TYPE = DD.TICKET_TYPE

    order by DD.DATE_VALUE asc, DD.TICKET_TYPE asc
;



-- Let's quickly explore the data to see what we're working with, the data is very peaky
-- which corresponds to big increases in ticket sales over the weekends, then big blocks
-- of increases over school holiday periods.
select * from TICKET_SALES_ENRICHED where DATE_VALUE >= '2024-01-01';



-- That's great, but what if we want to do something a bit more complicated, and I'm not
-- sure of the SQL syntax? Co-Pilot to the rescue! Let's ask the question:
-- "What is the average TOTAL_AMOUNT in TICKET_SALES for February 2024 vs February 2023"










-- EXAMPLE 1: FORECASTING ON A SINGLE TIME SERIES
-- https://docs.snowflake.com/en/user-guide/ml-functions/forecasting
-- https://docs.snowflake.com/sql-reference/classes/forecast/commands/create-forecast

-- Preparing Training Data: Let's try and predict the number of tickets purchased by our 
-- SENIOR customers. To do that we'll need a training dataset which is just the ticket 
-- sales for seniors.

create or replace view TICKET_SALES_ENRICHED_TRAINING_SENIORS as 
    select 
        DATE_VALUE::timestamp_ntz as DATE_VALUE_TS, -- Create FORECAST requires timestamps.
        sum( QTY ) as QTY,
        
    from TICKET_SALES_ENRICHED

    where TICKET_TYPE = 'Senior'

    group by all
;



-- Let's create the model and train it, it takes about 1m 30s on an XSmall so lets have a QUIZ!
create or replace SNOWFLAKE.ML.FORECAST TICKET_SALES_SENIORS(
    input_data => table( TICKET_SALES_ENRICHED_TRAINING_SENIORS ),
    timestamp_colname => 'DATE_VALUE_TS',
    target_colname => 'QTY'
);



-- Now lets test the model.
select 
    'Actual' as "Actual or Forecast",
    DATE_VALUE_TS::date as "Date",
    QTY as "Quantity",
    null as "Forecast",
    null as "Lower Bound",
    null as "Upper Bound",
    
from TICKET_SALES_ENRICHED_TRAINING_SENIORS where DATE_VALUE_TS >= '2024-01-01 00:00:00'

union all

-- This is where the model we created above is used to predict the future!
select 
    'Forecast',
    TS::date,
    null,
    FORECAST,
    LOWER_BOUND,
    UPPER_BOUND,
    
from table( TICKET_SALES_SENIORS!FORECAST( 
    forecasting_periods => 60
    , config_object => {'prediction_interval': 0.25}
) );



-- But what actually happened? Which features are most important for the model? The following 
-- helper functions allw you to assess your model performance, understand which features are 
-- most impactful to your model, and to help you debug the training process.
call TICKET_SALES_SENIORS!SHOW_EVALUATION_METRICS();



-- You can list all the models using the show command, we should have just the one.
show SNOWFLAKE.ML.FORECAST;










-- EXAMPLE 2: FORECASTING USING A MULTIPLE TIME SERIES WITH EXOGENOUS VARIABLES
-- https://docs.snowflake.com/en/user-guide/ml-functions/forecasting
-- https://docs.snowflake.com/sql-reference/classes/forecast/commands/create-forecast

-- Now we have an idea of what the data looks like, we can split it into data used for 
-- TRAINING and data used for PREDICTING. We'll keep 30 days worth of data for predicting

create or replace view TICKET_SALES_ENRICHED_TRAINING as 
    select 
        to_timestamp_ntz( DATE_VALUE ) as ML_DATE_POINT,
        * EXCLUDE ( DATE_VALUE, CINEMA_ID, TOTAL_AMOUNT ),
        
    from TICKET_SALES_ENRICHED

    where 1=1
        and DATE_VALUE <= dateadd( day, -30, current_date() )

    order by ML_DATE_POINT asc, TICKET_TYPE asc
;



create or replace view TICKET_SALES_ENRICHED_PREDICTING as 
    select 
        to_timestamp_ntz( DATE_VALUE ) as ML_DATE_POINT,
        * EXCLUDE ( DATE_VALUE, CINEMA_ID, TOTAL_AMOUNT ),
        
    from TICKET_SALES_ENRICHED

    where 1=1
        and DATE_VALUE between dateadd( day, -29, current_date() ) and dateadd( day, 60, current_date() )

    order by DATE_VALUE asc, TICKET_TYPE asc
;



-- What's each one got?
select TICKET_TYPE, count(*) from TICKET_SALES_ENRICHED group by all;
select TICKET_TYPE, count(*) from TICKET_SALES_ENRICHED_TRAINING group by all;
select TICKET_TYPE, count(*) from TICKET_SALES_ENRICHED_PREDICTING group by all;
select max( ML_DATE_POINT ) from TICKET_SALES_ENRICHED_PREDICTING group by all;



-- 1m 16s on a Medium Snowpark Optimized Warehouse
use warehouse SNOWPARK_OPTIMIZED_WH;



-- Let's create the model and train it, it takes about 1m 30s on an XSmall so lets have a QUIZ!
create or replace SNOWFLAKE.ML.FORECAST TICKET_SALES_QTY_FORECAST(
    input_data => system$reference( 'VIEW', 'TICKET_SALES_ENRICHED_TRAINING' ),
    timestamp_colname => 'ML_DATE_POINT',
    series_colname => 'TICKET_TYPE',
    target_colname => 'QTY',
    config_object => { 'ON_ERROR': 'SKIP' }
);



-- Suspend so we don't waste momey.
alter warehouse SNOWPARK_OPTIMIZED_WH suspend;



-- Swap back to the COMPUTE_WH.
use warehouse COMPUTE_WH;



-- Let's test it for senior tickets!
select 
    'Actual' as "Actual or Forecast", 
    DATE_VALUE as "Date", 
    TICKET_TYPE as "Ticket Type", 
    QTY as "Quantity",
    null as "Forecast", null as "Lower Bound", null as "Upper Bound",
    
from TICKET_SALES_ENRICHED 
where DATE_VALUE >= '2024-01-01 00:00:00' and TICKET_TYPE = 'Senior'

union all

select 
    'Forecast', 
    TS::date, 
    replace( SERIES, '"', '' ), 
    null,
    FORECAST, LOWER_BOUND, UPPER_BOUND,
    
from table( TICKET_SALES_QTY_FORECAST!FORECAST( 
    input_data => system$reference( 'VIEW', 'TICKET_SALES_ENRICHED_PREDICTING' ),
    timestamp_colname => 'ML_DATE_POINT',
    series_colname => 'TICKET_TYPE',
    
    -- Here we set your prediction interval.
    config_object => {'prediction_interval': 0.95}
) )

where replace( SERIES, '"', '' ) = 'Senior'

order by 1, 2, 3;



-- Inspect the accuracy metrics of your model. 
call TICKET_SALES_QTY_FORECAST!EXPLAIN_FEATURE_IMPORTANCE();



-- Or we could just use ML Studio...









/*********************************************************************************************************

██╗     ██╗     ███╗   ███╗    ███████╗██╗   ██╗███╗   ██╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗███████╗
██║     ██║     ████╗ ████║    ██╔════╝██║   ██║████╗  ██║██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██╔════╝
██║     ██║     ██╔████╔██║    █████╗  ██║   ██║██╔██╗ ██║██║        ██║   ██║██║   ██║██╔██╗ ██║███████╗
██║     ██║     ██║╚██╔╝██║    ██╔══╝  ██║   ██║██║╚██╗██║██║        ██║   ██║██║   ██║██║╚██╗██║╚════██║
███████╗███████╗██║ ╚═╝ ██║    ██║     ╚██████╔╝██║ ╚████║╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║███████║
╚══════╝╚══════╝╚═╝     ╚═╝    ╚═╝      ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝
                                                                                                         
*********************************************************************************************************/


-- Snowflake Cortex gives you instant access to industry-leading large language models (LLMs) trained by 
-- researchers at companies like Mistral, Reka, Meta, and Google, including Snowflake Arctic, an open 
-- enterprise-grade model developed by Snowflake.

-- Snowflake Cortex features are provided as SQL functions and are also available in Python. The 
-- available functions are summarized below.

    -- SENTIMENT: Returns a sentiment score, from -1 to 1, representing the detected positive or negative sentiment of the given text.
    -- COMPLETE: Given a prompt, returns a response that completes the prompt. This function accepts either a single prompt or a conversation with multiple prompts and responses.
    -- EXTRACT_ANSWER: Given a question and unstructured data, returns the answer to the question if it can be found in the data.
    -- SUMMARIZE: Returns a summary of the given text.
    -- TRANSLATE: Translates given text from any supported language to any other.
    -- EMBED_TEXT_768: Given a piece of text, returns a vector embedding of 768 dimensions that represents that text.
    -- EMBED_TEXT_1024: Given a piece of text, returns a vector embedding of 1024 dimensions that represents that text.









    
-- EXAMPLE 1: TESTING OUT BASIC LLM FUNCTIONS

-- Lets test SENTIMENT out
select SNOWFLAKE.CORTEX.SENTIMENT(
    'I really hate the book The Road. I loved No Country for Old Men, and thought I\'d enjoy The Road, but its a truely terrifying, depressing book!'
) as SENTIMENT;


select SNOWFLAKE.CORTEX.SENTIMENT(
    'I loved the book The Road, what a wonderful, happy story!'
) as SENTIMENT;



-- Lets test COMPLETE
select SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Critique this review in bullet points: <review>I''m currently a freshman in college, but I read this book during the second semester of my sophomore year of high school, which is when the coronavirus kind of started getting big.  Our teacher offered up six books, and everyone in the class got to choose which one they wanted to read.  I was the ONLY person to pick this novel, and I''m glad that I did.  It''s a great book and I could draw a lot of comparisons between it and the world I was living in three years ago. It''s a dark, and depressing novel, but there are glimmers of light sprinkled within it. The bond between father and son is a strong one, it made me think of my dad, and the sacrifices he made for me. It''s a good book.</review>'
) as COMPLETED;



-- Lets test EXTRACT_ANSWER
set QUESTION = 'I''m currently a freshman in college. I read The Road during the second semester of my sophomore year of high school during coronavirus. I was the ONLY person to pick this novel, and I''m glad that I did. 
The bond between father and son is a strong one.';

select SNOWFLAKE.CORTEX.EXTRACT_ANSWER( $QUESTION, 'What is the book called?' ) as EXTRACT_ANSWER;

select SNOWFLAKE.CORTEX.EXTRACT_ANSWER( $QUESTION, 'What was happening when they read The Road?' ) as EXTRACT_ANSWER;

select SNOWFLAKE.CORTEX.EXTRACT_ANSWER( $QUESTION, 'What year in high school were they in?' ) as EXTRACT_ANSWER;

select SNOWFLAKE.CORTEX.EXTRACT_ANSWER( $QUESTION, 'Did anyone else pick The Road to read?' ) as EXTRACT_ANSWER;

select SNOWFLAKE.CORTEX.EXTRACT_ANSWER( $QUESTION, 'Were they glad they chose the book?' ) as EXTRACT_ANSWER;



-- Lets test TRANSLATE
select SNOWFLAKE.CORTEX.TRANSLATE(
    'I''m currently a freshman in college, but I read this book during the second semester of my sophomore year of high school, which is when the coronavirus kind of started getting big.  Our teacher offered up six books, and everyone in the class got to choose which one they wanted to read.  I was the ONLY person to pick this novel, and I''m glad that I did.  It''s a great book and I could draw a lot of comparisons between it and the world I was living in three years ago.  It''s a dark, and depressing novel, but there are glimmers of light sprinkled within it.  The bond between father and son is a strong one, it made me think of my dad, and the sacrifices he made for me. It''s a good book.',
    'en',
    'fr'
) as TRANSLATED_TEXT;



-- Lets test SUMMARIZE
select SNOWFLAKE.CORTEX.SUMMARIZE(
    'I''m currently a freshman in college, but I read this book during the second semester of my sophomore year of high school, which is when the coronavirus kind of started getting big.  Our teacher offered up six books, and everyone in the class got to choose which one they wanted to read.  I was the ONLY person to pick this novel, and I''m glad that I did.  It''s a great book and I could draw a lot of comparisons between it and the world I was living in three years ago.  It''s a dark, and depressing novel, but there are glimmers of light sprinkled within it.  The bond between father and son is a strong one, it made me think of my dad, and the sacrifices he made for me. It''s a good book.'
) as SUMMARIZED_TEXT,
len( SUMMARIZED_TEXT );




-- Now we know how the basics work, let's save us a whole bunch of work and translate, summarise and 
-- perform sentiment analysis on the reviews. First we need to make sure we have an English language
-- version of the reviews.
update UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS
    set 
        REVIEW_EN = SNOWFLAKE.CORTEX.TRANSLATE( REVIEW, LANG, 'en' )
    where
        NAME = NAME
;



update UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS
    set 
        SENTIMENT = SNOWFLAKE.CORTEX.SENTIMENT( REVIEW_EN ),
        REVIEW_EN_SUMMARY = SNOWFLAKE.CORTEX.SUMMARIZE( REVIEW_EN )
    where
        NAME = NAME
;



select * from UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS;









-- EXAMPLE 2: BUILDING A STREAMLIT APP
-- OK lets swap to Streamlit and get building the app!
-- To use the examples below, open up a new browser tab and create a new Streamlit application.
-- Delete all the auto-generated code, then copy and paste each section of code below, one at a time
--  so you can see the app being built up over time.
-- ***** PLEASE NOTE: Don't copy the lines that are commented out using SQL syntax, i.e. lines 
--                    starting with -- or // or /* or */

-- ------------------------
-- STEP 1:
/*
# Import python packages
import streamlit as st
import numpy as np
import json
from snowflake.snowpark import functions as spf
from snowflake.snowpark.context import get_active_session



# Get the current credentials
session = get_active_session()



# Page header.
st.title( 'AI Driven Cinema!' )



# Get the reviews and strip out the single quotes as they break SQL and 
# we need it for Cortex later.
reviews_df = session.table('UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS').to_pandas()
reviews_df['CORTEX_SAFE_REVIEW'] = reviews_df['REVIEW'].str.replace( "'", "''" )



# Show what we got.
st.dataframe( reviews_df )
*/






-- ------------------------
-- STEP 2:
/*
# Everyone loves emojis, add some based on the sentiment then create the title which
# will be used to select the review.
sentiment_conditions = [ reviews_df['SENTIMENT'] > 0.5, reviews_df['SENTIMENT'] > 0.0 ]
sentiment_outputs = [ ' 😊 ', ' 🤨 ' ]
flag_conditions = [ reviews_df['LANG'] == 'en', reviews_df['LANG'] == 'fr' ]
flag_outputs = [ ' 🇬🇧 ', ' 🇫🇷 ' ]
reviews_df['REVIEW_DATE_STR'] = reviews_df['REVIEW_DATE'].astype(str)



# Add a column to the reviews_df which concatenates a few things and puts some emjois in.
reviews_df['REVIEW_TITLE'] = \
    reviews_df['REVIEW_DATE_STR'] + ' | ' + \
    reviews_df['REVIEW'].str[:40] + \
    ' ... ' + \
    np.select( sentiment_conditions, sentiment_outputs, ' 🤬 ' ) + \
    np.select( flag_conditions, flag_outputs, ' 🏴 ' )



# Show what we got.
st.dataframe( reviews_df['REVIEW_TITLE'] )
*/






-- ------------------------
-- STEP 3:
/*
# Create a drop down allowing the user to select a review, once selected put into a variable.
options_df = reviews_df['REVIEW_TITLE']

selected_review = st.selectbox( 
    label = 'Which review would you like to look at?',
    options = range( len( options_df ) ),
    format_func = lambda x: options_df[x],
    key = 'selected_review'
)
*/



-- ------------------------
-- STEP 4: START DISPLAYING THE REVIEW DETAILS
/*
# Pull out the values from the dataframe into individual variables.
review = reviews_df.iloc[selected_review]['REVIEW']
review_summary = reviews_df.iloc[selected_review]['REVIEW_EN_SUMMARY']
review_sentiment = reviews_df.iloc[selected_review]['SENTIMENT']
review_author = reviews_df.iloc[selected_review]['NAME']
review_email = reviews_df.iloc[selected_review]['EMAIL']
review_en = reviews_df.iloc[selected_review]['REVIEW_EN']
review_date = reviews_df.iloc[selected_review]['REVIEW_DATE']
review_has_offer = True if reviews_df.iloc[selected_review]['OFFER'] is not None else False
cortex_safe_review = reviews_df.iloc[selected_review]['CORTEX_SAFE_REVIEW']


# Display the review.
st.divider()
st.header( review_author + ' (' + review_email   + ')' )
st.metric( label='Sentiment Score', value="{:,.2f}".format(float(review_sentiment)) )
st.subheader( 'Review Summary' )
st.write( review_summary )
st.subheader( 'The Review in English' )
st.write( review_en )
st.subheader( 'The Original Review' )
st.write( review )
*/



-- ------------------------
-- STEP 5:
/*
# Lets generate some special offers!
if review_has_offer == False:
    def create_prompt( the_review ):
      
        prompt = f"""
               Generate an email from the cinema to the customer explaining an offer for 10% discounted 
               food if the sentiment of the review was happy and 25% if the sentiment was unhappy.
               Only generate one offer.
               The review is between the <review> and </review> tags.
               If you don´t have the information just say so.
               You don't need to put the actual review in the email.
               The offer code should be {review_date} in unix epoch time format, prefixed with a random code no more than four characters long.
               To use the offer code the customer should present this email at reception.
               The offer code should be wrapped in <>.
               Add emojis to make it exciting if you can.
               
               <review>  
               {the_review}
               </review>
               Answer: 
               """
    
        return prompt
    
    
    
    def ask_cortex( review_en ):
    
        prompt = create_prompt( review_en )
        cmd = 'select snowflake.cortex.complete(?, ?) as response'
        
        # df_response = session.sql( cmd, params=[st.session_state.model_name, prompt]).collect()
        df_response = session.sql( cmd, params=[model_name, prompt]).collect()
        
        return df_response
    
    
    
    
    
    st.divider()
    st.header( 'Generate a Personal Offer' )
    st.write( 'Choose your LLM then simply click Go!' )
    
    model_name = st.selectbox( 'Select your model:', session.table('UCKFIELD_CINEMA.PUBLIC.LLM') )
    
    if st.button("Generate Offer! ❄️"):
        question = str( review_en ).replace("'","")
        
        response = ask_cortex( question )
        
        st.write( response[0].RESPONSE )
    
        reviews_table = session.table("UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS")
        reviews_table.update({"OFFER": response[0].RESPONSE}, reviews_table["NAME"] == review_author )

else:
    st.info('This review has already had an offer generated!', icon="ℹ️")


# Debug
with st.expander( 'Debug' ):
    st.write( st.session_state )
*/




-- Just for fun, you can try generating an offer for each review by using the code below.
alter warehouse COMPUTE_WH set warehouse_size = MEDIUM;

select 
    RVW.NAME,
    RVW.EMAIL,
    RVW.SENTIMENT,
    LLM.NAME,
    SNOWFLAKE.CORTEX.COMPLETE(
        LLM.NAME,
        concat( 
            'Generate an email from the cinema to the customer explaining an offer for 10% discounted food if the sentiment of the review was happy and 25% if the sentiment was unhappy. Only generate one offer.:<review>', 
            RVW.REVIEW_EN,
            '</review>'
        )
    ) as COMPLETE

from UCKFIELD_CINEMA.CRM.CUSTOMER_REVIEWS as RVW
    
    join UCKFIELD_CINEMA.PUBLIC.LLM as LLM
        on LLM.CORTEX_FUNCTION = 'COMPLETE'

where 1=1
    and RVW.NAME in ( 'Dynamo Snare', 'Chronotrapper' )
;


