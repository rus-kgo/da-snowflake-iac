
import uuid
import yaml
import os



# Check if pipeine_id_tag exists
definition_files = os.listdir("definitions")

modified = False
for file in definition_files:
    # Load each definition yaml file as a dictionary
    with open(f'definitions/{file}') as f:
        definition = yaml.safe_load(f)
        if definition:
            # Convert the dictiory key into a string
            object = "".join(definition.keys())
            for i in definition[object]:
                try:
                    if not i['pipeline_id_tag']:
                        i['pipeline_id_tag'] = str(uuid.uuid4())
                        modified = True
                        print(f"Updated {i['name']} with pipeline_id_tag: {i['pipeline_id_tag']}")
                except KeyError:
                    raise Exception(f"Definition `{i['name']}` in the `{file}` is missing the `pipeline_id_tag` field.")
            
            # Write back the modified dictionary to the YAML file
            if modified:
                with open(f"definitions/{file}", 'w') as f:
                    yaml.dump(
                        definition,
                        f,
                        default_flow_style=False,
                        allow_unicode=True,
                        sort_keys=False,
                    )
                    print(f"Updated file: {file}")