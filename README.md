# dbt-jaffleshop
Welcome to my implementation of the Jaffle Shop! While the data is about toasted sandwiches, this project is where I've been honing my skills in Analytics Engineering. It’s my "sandbox" for turning messy raw data into clean, actionable insights using dbt.

## Analytics Engineering Porfolio Project
I built this to master the "dbt way" of doing things. It’s one thing to write a SQL query; it's another to build a scalable, tested, and documented data pipeline. This repository represents my transition from writing scripts to building data products.

I followed a modular structure to keep the logic "DRY" (Don't Repeat Yourself):

- Staging Layer: Where I "clean the kitchen." I rename columns for clarity and cast data types so they play nice together.

- Intermediate Layer: This is where the magic happens: joining orders and payments to create a clear picture of every transaction.

- Marts Layer: The final "menu." Tables like dim_customers allow anyone to see a customer’s lifetime value without digging through raw logs.

### Tech Stack:
- dbt cloud
- databricks
- deltalake

### dbt Packages:
- code_gen
- dbt_utils
- audit_helper

### Let's connect!
I'm always looking to chat about data modeling, SQL optimization, or the best fillings for a Jaffle (I'm partial to classic cheese and tomato).

GitHub: @dannykhant

LinkedIn: https://www.linkedin.com/in/dannykhant
