"""Main entrance point of the pipeline.

This module provides:
- main: main function that orchestrates the pipeline;
"""

import yaml 
import os
import re

from utils import Utils
from errors import DefinitionKeyError, TemplateFileError



def main(session, definitions_folder:str, resources_folder:str):
    """Entry point of the program."""
    utils = Utils(
        definitions_folder=definitions_folder,
        resources_folder=resources_folder,
    )

    # First check if all definitions have their tags, assign a tag if not
    utils.assign_pipeline_tag_id()

    # Map the dependencies of all the definitions in the yaml files
    map = utils.dependencies_map()

    # Do topographic sorting of the dependecies
    sorted_map = utils.dependencies_sort(map)["map"]

    definitions_path = os.path.join(os.getcwd(), definitions_folder)

    for i in sorted_map:
        object, object_name = i.split("::")

        file_path = os.path.join(definitions_path,f"{object}.yml")

        try:
            with open(file_path) as f:
                definition = yaml.safe_load(f)
        except FileNotFoundError:
            raise DefinitionKeyError(object) 
        
        for d_state in definition[object]:
            # This is for benefit of following the sorter order
            if d_state["name"] == object_name:

                # Here we are getting the tag to compare it with the snowfalke object instead o using the name
                object_id_tag = d_state["object_id_tag"]

                # Making sure the object is correctly named `database role` instead of `database_role`
                sf_object = re.sub(r"_", " ", object)

                # Run snowflake function to retrieve the object details as dictionary
                sf_state = utils.snowflake_state(session=session, object=sf_object, object_id_tag=object_id_tag)

                # Check if the object in the definition exists in Snowflake
                try:
                    if sf_state == {}:
                        print(
                            utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="CREATE",
                                ),
                        )

                    # If the object exists, but the definition has a new name, alter name
                    elif sf_state["name"] != d_state["name"]:

                        alter_definition = utils.state_comparison(new_state=d_state, old_state=sf_state)

                        print(
                            utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="ALTER",
                                new_name=alter_definition["name"],
                                old_name=sf_state["name"],
                                ),
                        )

                    # If the object exists and has the same name, alter the properties of the object
                    elif sf_state["name"] == d_state["name"]:

                        alter_definition = utils.state_comparison(new_state=d_state, old_state=sf_state)

                        print(
                            utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="ALTER",
                                ),
                        )
                except Exception as e:
                    raise TemplateFileError(object, folder=resources_folder, error=e)


if __name__ == "__main__":
    main(
        session="",
        definitions_folder="definitions",
        resources_folder="resources",
    )



                