import pandas as pd

from sqlalchemy import create_engine,text

# parsing date and account number from header
def parse_date_and_account_number(header_lines):
    date_str = header_lines[0][95:102].strip()
    account_number = header_lines[1][35:45].strip()
    return date_str, account_number

# validating the record is summary record or not
def is_summary_set(lines, index):
    line = lines[index].strip()
    if index + 1 < len(lines):
        next_line = lines[index + 1]
        cu_sip_candidate = line[-9:]
        if cu_sip_candidate.isalnum() and len(cu_sip_candidate) == 9:
            if "SECURITY TOTAL =>" in next_line:
                return True
    return False

# parsing trade records
def parse_trade_set(trade_set_lines, date_str, account_number):
    line1 = trade_set_lines[0] if len(trade_set_lines) > 0 else ""
    line2 = trade_set_lines[1] if len(trade_set_lines) > 1 else ""
    line3 = trade_set_lines[2] if len(trade_set_lines) > 2 else ""
    line4 = trade_set_lines[3] if len(trade_set_lines) > 3 else ""

    return {
        "for": date_str,
        "account": account_number,
        "security_description": line1[:20] +' '+ line2[:20].strip() +' '+ line3[:20].strip() +' '+ line4[:20].strip(),
        "lot_quantity": line1[22:35].strip(),
        "trade_date": line1[36:44].strip(),
        "settlement_date": line2[36:44].strip(),
        "execution_date": line1[46:54].strip(),
        "ref_no": line2[45:54].strip(),
        "price": line1[55:64].strip(),
        "open_amount": line1[65:79].strip(),
        "current_price": line1[80:89].strip(),
        "current_market_value": line1[90:105].strip(),
        "unrealized_p_and_l": line1[106:119].strip(),
        "trade_int": line1[120:131].strip(),
        "accrued_int": line2[120:131].strip(),
    }

#parsing summary records
def parse_summary_set(summary_set_lines, date_str, account_number):
    line1 = summary_set_lines[0]
    line2 = summary_set_lines[1]
    line3 = summary_set_lines[2]

    return {
        "for": date_str,
        "account": account_number,
        "security_description": line1[:20].strip(),
        "cu_sip": line1[22:35].strip(),
        "lot_quantity": line2[22:35].strip(),
        "open_amount": line2[65:79].strip(),
        "current_market_value": line2[90:105].strip(),
        "unrealized_p_and_l": line2[106:119].strip(),
        "trade_int": line2[120:131].strip(),
        "accrued_int": line3[120:131].strip(),
    }

# after parsing checks for any missing description
def assign_missing_trade_descriptions(trade_details):
    last_known_description = None
    for trade in trade_details:
        if trade["security_description"].strip():
            last_known_description = trade["security_description"]

        else:
            if last_known_description is not None:
                trade["security_description"] = last_known_description

            else:
                trade["security_description"] = "UNKNOWN_DESCRIPTION"

    return trade_details

# missing descriptions are assigned here
def assign_missing_descriptions_in_set(trades_in_set):
    last_known_description = None
    for trade in trades_in_set:
        if trade["security_description"].strip():
            last_known_description = trade["security_description"]
        else:
            trade["security_description"] = last_known_description or "UNKNOWN_DESCRIPTION"
    return trades_in_set

def convert_to_float(value):
    value = str(value).replace(",","")

    if value.endswith("-"):
        value = value[:-1]
        value = float(value) * -1

    else:
        value = float(value)

    return value

def convert_to_int(value):
    value = str(value).replace(",","")

    if value.endswith("-"):
        value = value[:-1]
        value = int(value) * -1

    else:
        value = int(value)

    return value

def insert_to_sql(trade_df, summary_df):
    dialect_and_driver = "mysql+pymysql:"
    username = "root"
    password = "root"
    host = "localhost"
    port = 3306
    db_name = "all_trades"

    engine = create_engine(f"{dialect_and_driver}//{username}:{password}@{host}:{port}/{db_name}")

    with engine.connect() as connection:
        #creating trade_details table
        connection.execute(text("""
            CREATE TABLE trade_details(
                id INT AUTO_INCREMENT PRIMARY KEY,
                for_date DATE,
                account VARCHAR(20),
                security_description VARCHAR(255),
                cu_sip VARCHAR(20),
                lot_quantity INT,
                trade_date DATE,
                settlement_date DATE,
                execution_date DATE,
                ref_no VARCHAR(15),
                price FLOAT,
                open_amount FLOAT,
                current_price FLOAT,
                current_market_value FLOAT,
                unrealized_p_and_l FLOAT,
                trade_int FLOAT,
                accrued_int FLOAT
            );
        """))
        #creating summary_details table
        connection.execute(text("""
            CREATE TABLE summary_details(
                id INT AUTO_INCREMENT PRIMARY KEY,
                for_date DATE,
                account VARCHAR(20),
                security_description VARCHAR(255),
                cu_sip VARCHAR(20),
                lot_quantity INT,
                open_amount FLOAT,
                current_market_value FLOAT,
                unrealized_p_and_l FLOAT,
                trade_int FLOAT,
                accrued_int FLOAT
            );
        """))

        #inserting trade details to trade table
        trade_df.to_sql(con=engine, name="trade_details", if_exists="append", index=False, chunksize=100)
        print("Trade Details inserted successfully.")

        #inserting summary details to summary table
        summary_df.to_sql(con=engine, name="summary_details", if_exists="append", index=False, chunksize=100)
        print("Summary Details inserted successfully.")

        connection.commit()


def data_conversion(all_trade_details, summary_details):
    trade_df = pd.DataFrame(all_trade_details)
    summary_df = pd.DataFrame(summary_details)

    # updating all date columns in trade_df

    summary_df["for"] = pd.to_datetime(summary_df["for"], format="%m/%d/%y").dt.date

    trade_df["for"] = pd.to_datetime(trade_df["for"], format="%m/%d/%y").dt.date
    trade_df["trade_date"] = pd.to_datetime(trade_df["trade_date"], format="%y/%m/%d").dt.date
    trade_df["settlement_date"] = pd.to_datetime(trade_df["settlement_date"], format="%y/%m/%d").dt.date
    trade_df["execution_date"] = pd.to_datetime(trade_df["execution_date"], format="%y/%m/%d").dt.date

    # removing special symbols in account
    trade_df["account"] = trade_df["account"].str.replace("-", "")
    summary_df["account"] = summary_df["account"].str.replace("-", "")

    # updating all prices into float
    price_columns = ["price","open_amount","current_price","current_market_value","unrealized_p_and_l","trade_int","accrued_int"]

    #converting price values into float type
    for price_column in price_columns:
        trade_df[price_column] = trade_df[price_column].apply(convert_to_float)

        if price_column in summary_df.columns:
            summary_df[price_column] = summary_df[price_column].apply(convert_to_float)

    # converting lot_quantity values into int type
    trade_df["lot_quantity"] = trade_df["lot_quantity"].apply(convert_to_int)

    summary_df["lot_quantity"] = summary_df["lot_quantity"].apply(convert_to_int)

    trade_df = trade_df.rename(columns={"for": "for_date"})
    summary_df = summary_df.rename(columns={"for": "for_date"})

    insert_to_sql(trade_df, summary_df)

def parse_file_with_state_machine(data):
    programs = data.split("\f")
    all_trade_details = []
    summary_details = []

    pending_trade_block = [] # stores unfinished trade records
    pending_set_trades = [] # stores trade records and waits for cu-sip from summary record

    for program in programs:
        blocks = program.strip().split("\n\n")
        header_lines = blocks[0].split("\n")
        date_str, account_number = parse_date_and_account_number(header_lines)

        body_lines = "\n\n".join(blocks[1:]).split("\n")

        state = "START"
        current_trade_block = pending_trade_block
        current_set_trades = pending_set_trades
        last_known_description = None

        i = 0
        while i < len(body_lines):
            line = body_lines[i]

            if line.startswith("**") or line.startswith("ACCOUNT-TOTALS"):
                i += 1
                continue

            if state == "START":
                if is_summary_set(body_lines, i):

                    summary_block = body_lines[i:i + 3]
                    summary_data = parse_summary_set(summary_block, date_str, account_number)
                    summary_details.append(summary_data)

                    for trade in current_set_trades:
                        if not trade["security_description"].strip():
                            trade["security_description"] = last_known_description or "UNKNOWN_DESCRIPTION"
                        trade["cu_sip"] = summary_data["cu_sip"]
                        all_trade_details.append(trade)

                    current_set_trades.clear()

                    last_known_description = None
                    i += 3

                elif line:
                    current_trade_block = [line]
                    state = "TRADE"
                    i += 1

                else:
                    i += 1

            elif state == "TRADE":
                if is_summary_set(body_lines, i):
                    trade_data = parse_trade_set(current_trade_block, date_str, account_number)

                    if not trade_data["security_description"].strip():
                        trade_data["security_description"] = last_known_description or "UNKNOWN_DESCRIPTION"

                    else:
                        last_known_description = trade_data["security_description"]

                    current_set_trades.append(trade_data)
                    current_trade_block = []
                    state = "START"
                    continue

                elif line:
                    current_trade_block.append(line)
                    i += 1

                else:
                    if current_trade_block:
                        trade_data = parse_trade_set(current_trade_block, date_str, account_number)

                        if not trade_data["security_description"].strip():
                            trade_data["security_description"] = last_known_description or "UNKNOWN_DESCRIPTION"

                        else:
                            last_known_description = trade_data["security_description"]

                        current_set_trades.append(trade_data)
                        current_trade_block = []
                    state = "START"
                    i += 1

        if current_trade_block:
            pending_trade_block = current_trade_block

        if current_set_trades:
            pending_set_trades = current_set_trades

    data_conversion(all_trade_details, summary_details)


with open("FT60_ECL891008_20241223.txt", "r") as file: #
    read_file = file.read()

parse_file_with_state_machine(read_file)

