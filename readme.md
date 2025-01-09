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