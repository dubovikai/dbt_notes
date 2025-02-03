## Initial Setup

1. **Create a .env File**: Duplicate the `.env-example` file and rename it to `.env`. Adjust the variables to fit your needs.

2. **Install the DBT**

It is not necessary to up Google composer for most cases.
For vscode all recommended extensions need to be installed.
"dbt Power User" extension provides all functionality for working with DBT framework.


3. **Install the Google Composer Dev**:
   - start docker
   - `gcloud --version` - check if gcloud is installed; use `gcloud-install.bash` if it is not installed.
   - `composer-install.bash` - install the Composer-Dev CLI on your machine.

[Google's documentation](https://cloud.google.com/composer/docs/composer-2/run-local-airflow-environments)

 **Start Your Project**: Initialize and start your project by running the following commands: 
   - `composer-dev start` - This will set up the necessary Docker containers for your project.
   - `composer-dev stop` - Starts your Airflow environment locally. After executing this command, you can view your Airflow DAGs by accessing the Airflow UI at `http://localhost:8080`.

### How to Run DBT Commands to Build and Run a Specific Table

1. **Install Dependencies**:
   Use the following command to install all dependencies specified in the `packages.yml` file.
   ```sh
   dbt deps
   ```

2. **Build the Table**:
   To build the table and all its dependencies, use the following command. 
   ```sh
   dbt build --select +model_to_build
   ```

3. **Run the Specific Table**:
   After building, if you want to run the specific table without its dependencies, use:
   ```sh
   dbt run --select model_to_run
   ```
   
---
 
### How to Use SQLFluff for Linting and Fixing SQL Files

1. **Lint SQL Files**:
   Use the following command to lint SQL files in the specified directory. This will check for any style or syntax issues according to SQLFluff's rules.
   ```sh
   sqlfluff lint models/dbt_objects/...
   ```

2. **Fix SQL Files**:
   To automatically fix issues detected by SQLFluff, use the following command. This will apply automated corrections to the SQL files.
   ```sh
   sqlfluff fix models/dbt_objects/...
   ```

3. **Manual Corrections**:
   Some corrections may need to be done manually. For example, you may need to specify the address for a table or make other context-specific adjustments that SQLFluff cannot automate.
