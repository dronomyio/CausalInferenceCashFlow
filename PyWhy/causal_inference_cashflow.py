from neo4j import GraphDatabase
import pandas as pd
import logging
from sklearn.ensemble import RandomForestRegressor
from econml.dml import LinearDML

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Neo4j query to fetch financial data
NEO4J_QUERY = """
MATCH (c:Company)-[:HAS_CASH_FLOW]->(cf:CashFlow),
      (c)-[:HAS_BALANCE_SHEET]->(bs:BalanceSheet),
      (c)-[:HAS_INCOME_STATEMENT]->(inc:IncomeStatement)
RETURN cf.operating_cashflow AS operating_cashflow,
       cf.cashflow_from_investment AS investment_cashflow,
       cf.cashflow_from_financing AS financing_cashflow,
       inc.net_income AS net_income,
       bs.total_shareholder_equity AS total_shareholder_equity,
       inc.total_revenue AS total_revenue,
       bs.total_liabilities AS total_liabilities,
       bs.total_assets AS total_assets;
"""

# Verify Neo4j data
def verify_data_in_neo4j(driver):
    logging.info("Verifying data in Neo4j...")
    with driver.session() as session:
        labels = session.run("CALL db.labels()").value()
        relationships = session.run("CALL db.relationshipTypes()").value()
        logging.info(f"Available labels: {labels}")
        logging.info(f"Available relationships: {relationships}")

        # Check for expected labels
        expected_labels = ["Company", "CashFlow", "BalanceSheet", "IncomeStatement"]
        for label in expected_labels:
            if label not in labels:
                logging.warning(f"Missing label: {label}")

        # Check for expected relationships
        expected_relationships = ["HAS_CASH_FLOW", "HAS_BALANCE_SHEET", "HAS_INCOME_STATEMENT"]
        for rel in expected_relationships:
            if rel not in relationships:
                logging.warning(f"Missing relationship: {rel}")

# Fetch data from Neo4j
def fetch_data_from_neo4j():
    logging.info("Connecting to Neo4j...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        verify_data_in_neo4j(driver)  # Verify Neo4j data structure
        with driver.session() as session:
            logging.info("Fetching data from Neo4j...")
            result = session.run(NEO4J_QUERY)
            data = [record.data() for record in result]
            logging.info(f"Fetched {data} records from Neo4j.")
        logging.info(f"Fetched {len(data)} records from Neo4j.")
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"An error occurred while fetching data: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure
    finally:
        driver.close()

# Prepare data for analysis
def prepare_data(df):
    logging.info("Preparing data for analysis...")
    df.fillna(0, inplace=True)  # Replace nulls with zeros
    numeric_columns = df.select_dtypes(include=["number"]).columns
    df[numeric_columns] = df[numeric_columns].astype(float)  # Ensure numeric types
    logging.info("Data preparation complete.")
    return df

# Perform causal analysis
def perform_causal_analysis(df):
    logging.info("Performing causal analysis...")
    if df.empty:
        logging.warning("No data available for causal analysis.")
        return

    # Example causal analysis using GDP and Total Revenue
    logging.info("Estimating effect of GDP on Total Revenue using Double Machine Learning...")
    try:
        dml_model = LinearDML(
            model_y=RandomForestRegressor(),
            model_t=RandomForestRegressor(),
            random_state=42
        )
        dml_model.fit(
            y=df['total_revenue'],
            T=df['operating_cashflow'],
            X=df[['investment_cashflow', 'financing_cashflow']]
        )
        dml_effect = dml_model.effect(df[['investment_cashflow', 'financing_cashflow']])
        logging.info(f"Estimated effect of Operating Cash Flow on Total Revenue: {dml_effect.mean()}")
    except Exception as e:
        logging.error(f"An error occurred during causal analysis: {e}")


# Perform causal analysis
def perform_causal_analysis1(df):
    logging.info("Performing causal analysis...")
    if df.empty:
        logging.warning("No data available for causal analysis.")
        return

    # Example causal analysis using Operating Cash Flow and Total Revenue
    logging.info("Estimating effect of Operating Cash Flow on Total Revenue using Double Machine Learning...")
    try:
        dml_model = LinearDML(
            model_y=RandomForestRegressor(),
            model_t=RandomForestRegressor(),
            random_state=42
        )
        # Fit the DML model
        dml_model.fit(
            Y=df['total_revenue'],  # Outcome variable
            T=df['operating_cashflow'],  # Treatment variable
            X=df[['investment_cashflow', 'financing_cashflow']],  # Covariates
            W=None  # No instrument variable
        )
        # Estimate the effect
        dml_effect = dml_model.effect(df[['investment_cashflow', 'financing_cashflow']])
        logging.info(f"Estimated effect of Operating Cash Flow on Total Revenue: {dml_effect.mean()}")
    except Exception as e:
        logging.error(f"An error occurred during causal analysis: {e}")

# Main execution
if __name__ == "__main__":
    try:
        # Fetch data from Neo4j
        data = fetch_data_from_neo4j()

        # If data is retrieved, prepare and analyze it
        if not data.empty:
            prepared_data = prepare_data(data)
            logging.info(prepared_data)
            perform_causal_analysis1(prepared_data)
        else:
            logging.warning("No data retrieved. Exiting...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
