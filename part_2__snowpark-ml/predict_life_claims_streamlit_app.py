# Standard library imports
import pandas as pd



# Third party imports
import streamlit as st
from snowflake.snowpark import DataFrame
from snowflake.snowpark.context import get_active_session
from snowflake.ml.registry import Registry
from snowflake.ml import dataset



# Set where we're working.
db_name = 'INSURANCE'
schema_name = 'PUBLIC'



# Get the current credentials
session = get_active_session()



# Give it a header
st.header( 'Choose Your Model & Version' )



# Opens a registry within a Snowflake schema.
reg = Registry( 
    session = session, 
    database_name = db_name, 
    schema_name = schema_name 
)



# Create a selectbox allowing users to choose a model they want to use.
# Get the models list.
model_df = reg.show_models()
model_pdf = model_df[['name', 'created_on']]

# Create a select box.
model_select = st.selectbox(
    options = model_pdf['name'],
    label = 'Choose your model',
)

# Get the model object.
model = reg.get_model( db_name + '.' + schema_name + '.' + model_select )



# Get the versions list.
model_versions_df = model.show_versions()
model_versions_pdf = model_versions_df[['name', 'created_on']]



# Do some munging.
model_versions_pdf['created_on'] = model_versions_pdf['created_on'].dt.tz_convert(None)
model_versions_pdf['created_on_str'] = model_versions_pdf['created_on'].astype(str).str[:-4]
model_versions_pdf['model_key'] = model_versions_pdf['created_on_str'] + ' - ' + model_versions_pdf['name']



# Create a select box.
model_version_select = st.selectbox(
    options = model_versions_pdf['model_key'],
    label = 'Choose your model version',
)



# Get the version object.
model_version_chosen = model_versions_pdf.loc[ model_versions_pdf['model_key'] == model_version_select, 'name'].item()
mv = model.version( model_version_chosen )



# Get a dataset.
datasets_rows = session.sql( 'show datasets in schema' )
datasets_df = pd.DataFrame( datasets_rows.collect() )



# Create a select box.
dataset_select = st.selectbox(
    options = datasets_df['name'],
    label = 'Choose your dataset',
)



# Get the Dataset object.
dataset_object = dataset.Dataset( session, db_name, schema_name, dataset_select )


# Create a select box.
dataset_version_select = st.selectbox(
    options = dataset_object.list_versions(),
    label = 'Choose your dataset version',
)



# Get the dataset version.
dataset_version_object = dataset_object.select_version( dataset_version_select )


# Convert to a Snowpark DF.
# Get the dataset we created above and turn into a SP Dataframe.
client_features_df = dataset_version_object.read.to_snowpark_dataframe()


# Let's go predicting.
if st.button( '📈 Lets Go Predicting...' ):
    model_prediction = mv.run( client_features_df, function_name='predict' )
    st.dataframe( model_prediction.select(['AGE', 'SMOKER_ENCODED', 'TOTAL_CLAIMS', 'TOTAL_CLAIMS_PREDICTION']) )


