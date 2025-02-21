important fields to make sure are present in the definition file:
- object 
- name
- depends_on

```yaml
role: # object
  - name: aws_step_function # object name
    depends_on: # dependecies field, must be present even if left as None
```


for simplicity the file names and the object name in the yaml files must match the snowflake official object name"


# How it works

Snowflake SQL jinja templates are being rendere with the data from the defintions yaml files.
1. Reads the yaml files
2. Builds a topographic map of the snowflake objects defined in the yaml files for sorting the dependecies. 
3. An ordered list of objects (based on the dependecies) is created from the map.
4. Snowflake is being queried (show object/describe object) to the definition of already existing objects
5. The object'y yaml file state is being checked against the object's state in Snowflake. If there is a difference,  
   the iac_action would be to alter an ojbect. If the object does not exists in Snowflake it will be created base on the yaml file. 

You can do a dry-run, that would only print out the plan for execution.

You can use differen roles to create objects, since it will be using an OAuth authentification with ANY ROLE opiton. 

