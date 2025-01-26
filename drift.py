"""State comparison module.

This module performs the checks of snowflake objects drift.
Drift is the term for when the real-world state of your infrastructure differs from the state defined in your configuration.
"""

import pandas as pd
import json
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from snowflake.connector import SnowflakeConnection  # noqa: TC004



class ObjectDriftCheck:
    """Drift check of the snowflake objects."""

    def __init__(self, conn:SnowflakeConnection) -> dict:
        """Initialize the comparator with Snowflake connection parameters and YAML definitions file path.
        
        :param conn: Dictionary of Snowflake connection parameters.
        """
        self.conn = conn
        self.show_output = None
        self.desc_output = None

    def _flatten_value(self, value):
        if isinstance(value, list):
            if len(value) > 1:
                return f"{tuple(value)}"
            return value[0]
        if isinstance(value, dict):
            return str(value)  
        if value is None:
            return "null"
        return value  

    def query_fetch_to_df(self, query:str) -> pd.DataFrame:
        """Execute an asynchronous query in Snowflake and fetch results.

        :return: Query results.
        """
        if not self.conn:
            raise ConnectionError("No active Snowflake connection.")  # noqa: TRY003

        cursor = self.conn.cursor()

        try:
            cursor.execute(query)
            # Get the columns name of the query output
            cols = cursor.description
            df_cols = pd.DataFrame(cols)
            list_cols = df_cols["name"].to_list()

            # Get the query output
            show_object = cursor.fetchall()

            # Transform the output into a df
            df = pd.DataFrame(show_object, columns=list_cols)

        except Exception as e:
            print(f"Error executing query: {query}\n{e}")
            return None
        
        return df
        

    def object_state(
            self, 
            object_id:str, 
            object_definition:dict, 
            show_output:pd.DataFrame, 
            sf_object:str,
            describe_object:Literal["Yes","No"] = "Yes",
            ) -> dict:
        """Compare the state of an object in Snowflake with its YAML definition.

        :param object_id: Snowflake object_id taken from the yaml definition file.
        :param object_definition: Swnowflake object definition stored in the yaml file.
        :param show_output: Snowflake output when quering to show objects.
        :param describe_object: For some objects the DESCRIBE output does not include the object definition. Instead, only SHOW is used.
        :return: Comparison result as a dictionary.
        """
        # Get the name of the object corresponding to the id
        object_name = show_output[show_output["comment"].str.contains(object_id)]["name"]

        # Return if the the object does not exists in Snowflake
        if len(object_name) == 0:
            return None
        
        object_name= object_name.values[0]

        # Pivot to match the yaml definition
        df_show = show_output[show_output["comment"].str.contains(object_id)].melt(var_name="Property",value_name="Value")

        if describe_object == "Yes":
            query = f"describe {sf_object} like {object_name}"

            # Get description of the object
            df_desc = self.query_fetch_to_df(query=query)
            # Pivot for combination
            df_desc = df_desc.melt(var_name="Property", value_vars="Value")

            # Combine show and desc outputs
            sf_df = pd.concat([df_desc, df_show], ignore_index=True, join="inner").drop_duplicates()
        
        df_show = sf_df

        # Transform the comment json string into a dictionory
        comment = show_output[show_output["comment"].str.contains(object_id)]["comment"].values[0]
        comment = json.loads(comment)

        # Create new rows from the fields in the comments json string
        comment_rows = pd.DataFrame(comment.items(), columns=["Property", "Value"])

        # Filter out the comments json string containing a dict as we will add it to df separetely as new rows
        sf_df = sf_df[sf_df["Property"] != "comment"]

        # Add new rows to the dataframe
        sf_df = pd.concat([sf_df, comment_rows], ignore_index=True)

        d_df = pd.DataFrame({
            "Property": list(object_definition.keys()),
            "Value": [self._flatten_value(value) for value in object_definition.values()],    
        })

        # Filter out properties specific to the pipeline project
        d_df = d_df[~d_df["Property"].isin(["depends_on", "wait_time"])]

        # Set index to Properties
        sf_df = sf_df.set_index("Property")
        d_df = d_df.set_index("Property")

        # Set all string values to lower case before comparison as it is case sensetive
        sf_df = sf_df.map(lambda x : x.lower() if isinstance(x, str) else x)
        d_df = d_df.map(lambda x : x.lower() if isinstance(x, str) else x)

        # Ensure both DataFrames have the same index
        sf_df, d_df = sf_df.align(d_df, join="inner")

        # Use compare to find differences
        comparison = d_df.compare(sf_df, keep_shape=False, keep_equal=False, result_names=("new","old"))

        try:
            return comparison["Value"]["old"].to_dict()
        except KeyError:
            return comparison.to_dict()