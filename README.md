# sqliac

SQL Infrastructure  as Code


I need you to create SQL+Jinja ddl_template.sql and state.sql for each of the below Snowflake 
resources: 
- alert
- dynamic_table
- event_table
- grant
- notification_integration
- role
- schema
- security_integration
- stage
- storage_integration
- stream
- view
- task
- user
- warehouse
- dyanmice_iceberg_table

Refer to the examples in the project for database, schema and table.
Follow the same folder structure.
Once you have created SQL+Jinja template for each resource add their corresponding definitions in the config.toml following the existing pattern in the existing file. 

The main goal is for state.sql to output a single row of json object that will match the arguments of the ddl_context in the config.toml that would be fed in the the ddl_template.sql of each object. If the object does not exists, the state.sql should return an empty table, not fail.



I need you to create `ddl_template.sql` and `state.sql` for each of the following Snowflake resources. Ensure `state.sql` returns a single JSON object row matching the `ddl_context` arguments for `ddl_template.sql`, or an empty table if the resource does not exist.

**Snowflake Resources:**
- alert
- dynamic_table
- event_table
- grant
- notification_integration
- role
- schema
- security_integration
- stage
- storage_integration
- stream
- view
- task
- user
- warehouse
- dynamic_iceberg_table

**Instructions:**

1.  **Reference Existing Examples**:
    *   For `database`: `definitions/database.toml`
    *   For `schema`: `definitions/schema.toml`
    *   For `table`: `definitions/table.toml`

2.  **Follow Folder Structure**:
    *   Create a dedicated directory for each new resource under `provider/<resource_name>/`.
    *   Place `ddl_template.sql` and `state.sql` within each resource's directory.
    *   For example: `provider/alert/ddl_template.sql`

3.  **Implement `ddl_template.sql`**:
    *   Write the SQL Data Definition Language (DDL) using Jinja templating to create the respective Snowflake resource.
    *   The `ddl_template.sql` should expect Jinja variables that correspond to the arguments defined in the `ddl_context` of the resource's definition.

4.  **Implement `state.sql`**:
    *   Write SQL queries that retrieve the current configuration of the Snowflake resource from system views (e.g., `INFORMATION_SCHEMA`).
    *   The query must output a single row as a JSON object.
    *   The keys and values of this JSON object **must exactly match** the expected Jinja variables (arguments) for the `ddl_template.sql` for that resource.
    *   If the resource does not exist in Snowflake, the query should return an **empty result set (an empty table)**, not an error.

5.  **Update `config.toml`**:
    *   After creating the SQL templates for each resource, add its corresponding definition to `provider/config.toml`.
    *   Ensure the new definitions follow the existing pattern in the file for `database`, `schema`, and `table`.
    *   Pay close attention to the `ddl_context` structure, ensuring it aligns with the expected output of `state.sql` and the input for `ddl_template.sql`.

6.  **Adhere to Project Principles**:
    *   Prioritize minimizing complexity, implement CREATE OR ALTER for the resources that support it.
    *   Ensure names are clear and descriptive, explaining *why* decisions were made in comments rather than *what* the code does.
