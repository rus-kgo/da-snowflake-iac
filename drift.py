"""State comparison module.

This module performs the checks of snowflake objects drift.
Drift is the term for when the real-world state of your infrastructure differs from the state defined in your configuration.
"""

import pandas as pd
import json
from typing import Literal, Any
from snowflake.connector import SnowflakeConnection



class ObjectDriftCheck:
    """Drift check of the snowflake objects."""

    def __init__(self, conn:SnowflakeConnection) -> dict:
        """Initialize the comparator with Snowflake connection parameters and YAML definitions file path.
        
        :param conn: Dictionary of Snowflake connection parameters.
        """
        self.conn = conn

    def _flatten_nested_dict(self, data:dict[str, Any], nested_field:str|None) -> pd.DataFrame:
        """Convert a nested dictionary with a specified nested field into a flattened DataFrame.

        :param data: Dictionary containing both regular and nested fields
        :param nested_field: Name of the field containing nested dictionaries (default: "columns")
        :return:
            pandas.DataFrame: Flattened DataFrame with Property and Value columns
        """
        # Separate nested field from regular fields
        regular_items = {k: v for k, v in data.items() if k != nested_field}

        # Handle regular fields
        regular_rows = []
        for key, value in regular_items.items():
            if isinstance(value, list):
                value = f"{tuple(value)}" if len(value) > 1 else value[0]
            elif isinstance(value, dict):
                value = str(value)
            elif value is None:
                value = "null"
            regular_rows.append({
                "Property": key, 
                "Value": value,
                })

        # Handle nested fields
        nested_rows = []
        if nested_field and data[nested_field]:
            first_nested_item = data[nested_field][0]
            for key in first_nested_item:
                nested_rows.append({
                    "Property": f"{nested_field}_{key}",
                    "Value": first_nested_item[key],
                })
        
        # Combine and create DataFrame
        return pd.DataFrame(regular_rows + nested_rows)

            


    def query_fetch_to_df(self, query:str) -> pd.DataFrame:
        """Execute an asynchronous query in Snowflake and fetch results."""
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
            query_output = cursor.fetchall()

            # Transform the output into a df
            df = pd.DataFrame(query_output, columns=list_cols)

        except Exception as e:
            print(f"Error executing query: {query}\n{e}")
            return None
        
        return df
        

    def object_state(
            self, 
            object_id:str, 
            object_definition:dict, 
            sf_object:str,
            nested_field:str|None,
            describe_object:Literal["Yes","No"] = "Yes",
            ) -> dict:
        """Compare the state of an object in Snowflake with its YAML definition.

        :param object_id: Snowflake object_id taken from the yaml definition file.
        :param object_definition: Swnowflake object definition stored in the yaml file.
        :param describe_object: For some objects the DESCRIBE output does not include the object definition. Instead, only SHOW is used.
        :return: Comparison result as a dictionary.
        """
        show_output = self.query_fetch_to_df(f"show {sf_object}s")

        # Get the name of the object corresponding to the id
        object_name = show_output[show_output["comment"].str.contains(object_id)]["name"]

        # Return if the the object does not exists in Snowflake
        if len(object_name) == 0:
            return None
        
        object_name= object_name.values[0]

        # Pivot to match the yaml definition
        df_show = show_output[show_output["comment"].str.contains(object_id)].melt(var_name="property",value_name="property_value")

        if describe_object == "Yes":
            query = f"describe {sf_object} like {object_name}"

            # Get description of the object
            df_desc = self.query_fetch_to_df(query=query)

            # Prefix describe columns if the object has nested field
            if nested_field:
                df_desc.columns = [f"{nested_field}_" + col for col in df_desc.columns]
                # Pivot for combination
                df_desc = df_desc.melt(var_name="property", value_vars="property_value")

            # Select Only necessary Columns
            df_desc = df_desc[["property", "property_value"]]
            # Combine show and desc outputs
            sf_df = pd.concat([df_desc, df_show], ignore_index=True, join="inner").drop_duplicates()
        else: 
            sf_df = df_show

        # Transform the comment json string into a dictionory
        comment = show_output[show_output["comment"].str.contains(object_id)]["comment"].values[0]
        comment = json.loads(comment)

        # Create new rows from the fields in the comments json string
        comment_rows = pd.DataFrame(comment.items(), columns=["property", "property_value"])

        # Filter out the comments json string containing a dict as we will add it to df separetely as new rows
        sf_df = sf_df[sf_df["property"] != "comment"]

        # Add new rows to the dataframe
        sf_df = pd.concat([sf_df, comment_rows], ignore_index=True)

        # Convert the defintion dict into a df matching the snowflake output
        d_df = self._flatten_nested_dict(object_definition, nested_field=nested_field)

        # Filter out properties specific to the pipeline project
        d_df = d_df[~d_df["property"].isin(["depends_on", "wait_time"])]

        # Set index to Properties
        sf_df = sf_df.set_index("property")
        d_df = d_df.set_index("property")

        # Set all string values to lower case before comparison as it is case sensetive
        sf_df = sf_df.map(lambda x : x.lower() if isinstance(x, str) else x)
        d_df = d_df.map(lambda x : x.lower() if isinstance(x, str) else x)

        # Ensure both DataFrames have the same index
        sf_df, d_df = sf_df.align(d_df, join="inner")

        # Use compare to find differences
        comparison = d_df.compare(sf_df, keep_shape=False, keep_equal=False, result_names=("new","old"))

        try:
            return comparison["property_value"]["old"].to_dict()
        except KeyError:
            return comparison.to_dict()
