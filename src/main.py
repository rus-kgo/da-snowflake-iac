import yaml
import os
import re

from utils import Utils


def main(session, definitions_path, resources_path):

    try:
        utils = Utils(
            definitions_path=definitions_path,
            resources_path=resources_path
        )
    except:
        raise Exception(f"The file paths: {definitions_path = }, {resources_path = } might be wrong for current working directory: '{os.getcwd()}'")

    # First check if all definitions have their tags, assign a tag if not
    utils.assign_pipeline_tag_id()

    # Map the dependencies of all the definitions in the yaml files
    map = utils.dependencies_map()

    # Do topographic sorting of the dependecies
    sorted_map = utils.dependencies_sort(map)["map"]

    definitions_path = os.path.join(os.getcwd(), definitions_path)

    for i in sorted_map:
        object, object_name = i.split("::")

        file_path = os.path.join(definitions_path,f"{object}.yml")

        with open(file_path) as f:
            definition = yaml.safe_load(f)
        
        for d_state in definition[object]:
            # This is for benefit of following the sorter order
            if d_state['name'] == object_name:

                # Here we are getting the tag to compare it with the snowfalke object instead o using the name
                object_id_tag = d_state['object_id_tag']

                # Making sure the object is correctly named `database role` instead of `database_role`
                sf_object = re.sub(r'_', ' ', object)

                # Run snowflake function to retrieve the object details as dictionary
                sf_state = utils.snowflake_state(session="", object=sf_object, object_id_tag=object_id_tag)

                # Check if the object in the definition exists in Snowflake
                if sf_state == {}:
                    print(
                        utils.render_templates(
                            template_file=f"{object}.sql",
                            definition=d_state,
                            action="create"
                            )
                    )

                # If the object exists, but the definition has a new name, alter name
                elif sf_state['name'] != d_state['name']:

                    alter_definition = utils.state_comparison(new_state=d_state, old_state=sf_state)

                    print(
                        utils.render_templates(
                            template_file=f"{object}.sql",
                            definition=d_state,
                            action="alter",
                            new_name=alter_definition['name'],
                            old_name=sf_state['name']
                            )
                    )

                # If the object exists and has the same name, alter the properties of the object
                elif sf_state['name'] == d_state['name']:

                    alter_definition = utils.state_comparison(new_state=d_state, old_state=sf_state)

                    print(
                        utils.render_templates(
                            template_file=f"{object}.sql",
                            definition=d_state,
                            action="alter",
                            )
                    )

if __name__ == '__main__':
    main(
        session="",
        definitions_path="definitions",
        resources_path="resources"
    )



                