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


Can't use the query history as it holds info only up to one year https://select.dev/posts/snowflake-query-history


Use SQLAlchemy to establish the connection for oracle and snowflake
https://chatgpt.com/c/6873b4d5-36ac-800d-b8ac-0a62e72f3724

Not using id tags to identify object, this limitation is OK
Also not using different owners for each object, using one more sequire anyway.
These simplify things.
Probably not implementing warehouse size, since it is snowflake specific. 




[Normalize Object Definition](../drift_test.py/#L10) 
<details>
<summary>Click to expand</summary>

```gherkin
  Scenario: Normalize Object Definitions
    Given 
    When
    Then
```
</details>


# Requirement 1: Object Drift Detection

<details>
<summary>Test Cases</summary>

- `test_object_exists`
- `test_keys_match`
- `test_values_match`
- `test_create_if_missing`
- `test_alter_if_different`
</details>

```mermaid
flowchart TD
    R1[Requirement: Drift Check] --> T1[test_object_exists]
    R1 --> T2[test_keys_match]
    R1 --> T3[test_values_match]
    R1 --> T4[test_create_if_missing]
    R1 --> T5[test_alter_if_different]
```

Test coverage:
- `tests/test_drift_check.py::TestDriftCheck::test_no_action_matches`
