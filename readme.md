This script explains how fixed width text is parsed and that parsed text is inserted into mysql database

Requirements
-
- python3
- pandas
- sqlalchemy
- MySQL 

Dependencies Installation
-
    pip install pandas

    pip install sqlalchemy

Approach
- 
- State Machine Parsing Technique.
  - 
  - Define each state for each process or condition or input symbol.
  - If the input symbol becomes True, Then remain in the current state.
  - if the input symbol become False, Then move to next state.
  - The process of jumping from one state to other is called "Transition". And Transition depends on current state and input symbol



Steps for parsing fixed width text
- 
- Read the "text" file.
- split the records in "**program**" wise.
- In each program, split "**header**" and "**body**".
- Parse "**program date**" and "**account number**" from header.
- Read every record from program body.
- Define two states "**START**" and "**TRADE**". If the record is "**summary record**" add to "**summary_details**" . 
- if not, make a transition to "**TRADE**" state. If the record is not a summary record add to "**trade_details**".
- Add "**cu_sip**" value from summary_details to trade_details.
- If there exist any unfinished trade record in a program, pass it to the next program.
- Now, Convert summary_details and trade_details into dataframes using pandas library.

Saving Data into MySQL Database
-
- Make database connection with python by calling create_engine

        dialect_and_driver = "mysql+pymysql:"
        username = "root"
        password = "root"
        host = "localhost"
        port = 3306
        db_name = "all_trades"

        engine = create_engine(f"{dialect_and_driver}//{username}:{password}@{host}:{port}/{db_name}")

- Create tables for trade_details, summary_details using "**execute**".
- Insert the data into the tables.
- Save into the database using "**commit**".

