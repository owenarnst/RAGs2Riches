from sqlalchemy import create_engine, MetaData, Table, insert, text
import os
import pandas as pd

# Assuming the engine is already configured and imported
# engine = create_engine('your_database_url_here')


# Function to upload summaries
def upload_summaries(filepath):
    """
    Uploads a list of summaries to the database.

    :param summaries: A list of dictionaries containing summary details.
                      Example: [{'CompanyID': 1, 'Year': 2023, 'SummaryText': '...'}]
    """
    try:
        # Insert data into the table
        with engine.connect() as connection:
            with connection.begin():
                for filename in os.listdir(filepath):
                    print(f"Uploading {filename}...")
                    ticker = filename.split("_")[0]
                    row = tenK_2022[tenK_2022["Ticker"] == ticker]
                    company_id = int(row["CompanyID"].values[0])
                    year = 2022
                    with open(os.path.join(filepath, filename), "r") as file:
                        summary_text = file.read()
                    insert_stmt = insert(company_ten_k_summaries).values(
                        CompanyID=company_id, Year=year, SummaryText=summary_text
                    )

                    # Execute the insertion
                    connection.execute(insert_stmt)

        print("Summaries uploaded successfully.")
    except Exception as e:
        print(f"Error uploading summaries: {e}")


if __name__ == "__main__":

    tenK_2022 = pd.read_csv("tenK_2022.csv")

    DATABASE_URL = "postgresql+psycopg2://u381r20ceebmb7:p2c1b3eb128bb09f92c43d005d55f54c36a4a0e5bd110945652252726dfdb6068@c3gtj1dt5vh48j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/d77oud95l1v4g6"
    engine = create_engine(DATABASE_URL)

    metadata = MetaData()
    company_ten_k_summaries = Table(
        "company_ten_k_summaries", metadata, autoload_with=engine
    )

    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM company_ten_k_summaries"))
        for row in result:
            print(row)

    # dir = "/home/owenhustles/Desktop/StockPilot/summaries2022"
    # upload_summaries(dir)
