import mysql.connector
from neo4j import GraphDatabase
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MySQL and Neo4j credentials
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "SPYStocks",
}
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "Suvas1s22")  # Replace with your Neo4j username and password

# Fetch CASH_FLOW data from MySQL
def fetch_cash_flow_data():
    logging.info("Fetching cash flow data from MySQL...")
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Query to fetch cash flow data
        cursor.execute("""
            SELECT
                cf.*,
                c.id AS company_id
            FROM CASH_FLOW cf
            INNER JOIN Companies c ON cf.id = c.id
        """)

        data = cursor.fetchall()
        logging.info(f"Successfully fetched {len(data)} cash flow records.")
        return data

    except mysql.connector.Error as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        return []

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Insert CASH_FLOW data into Neo4j
def insert_cash_flow_data_to_neo4j(cash_flow_data):
    logging.info("Preparing cash flow data for Neo4j...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        with driver.session() as session:
            for record in cash_flow_data:
                # Prepare parameters for Neo4j
                params = {
                    "company_id": record["company_id"],
                    "cash_flow_id": record["cash_flow_id"],
                    "fiscal_date_ending": record["fiscal_date_ending"],
                    "reported_currency": record["reported_currency"],
                    "operating_cashflow": record.get("operating_cashflow", 0),
                    "payments_for_operating_activities": record.get("payments_for_operating_activities", 0),
                    "proceeds_from_operating_activities": record.get("proceeds_from_operating_activities", 0),
                    "change_in_operating_liabilities": record.get("change_in_operating_liabilities", 0),
                    "change_in_operating_assets": record.get("change_in_operating_assets", 0),
                    "depreciation_depletion_and_amortization": record.get("depreciation_depletion_and_amortization", 0),
                    "capital_expenditures": record.get("capital_expenditures", 0),
                    "change_in_receivables": record.get("change_in_receivables", 0),
                    "change_in_inventory": record.get("change_in_inventory", 0),
                    "profit_loss": record.get("profit_loss", 0),
                    "cashflow_from_investment": record.get("cashflow_from_investment", 0),
                    "cashflow_from_financing": record.get("cashflow_from_financing", 0),
                    "proceeds_from_repayments_of_short_term_debt": record.get("proceeds_from_repayments_of_short_term_debt", 0),
                    "payments_for_repurchase_of_common_stock": record.get("payments_for_repurchase_of_common_stock", 0),
                    "payments_for_repurchase_of_equity": record.get("payments_for_repurchase_of_equity", 0),
                    "payments_for_repurchase_of_preferred_stock": record.get("payments_for_repurchase_of_preferred_stock", 0),
                    "dividend_payout": record.get("dividend_payout", 0),
                    "dividend_payout_common_stock": record.get("dividend_payout_common_stock", 0),
                    "dividend_payout_preferred_stock": record.get("dividend_payout_preferred_stock", 0),
                    "proceeds_from_issuance_of_common_stock": record.get("proceeds_from_issuance_of_common_stock", 0),
                    "proceeds_from_issuance_of_long_term_debt": record.get("proceeds_from_issuance_of_long_term_debt", 0),
                    "proceeds_from_issuance_of_preferred_stock": record.get("proceeds_from_issuance_of_preferred_stock", 0),
                    "proceeds_from_repurchase_of_equity": record.get("proceeds_from_repurchase_of_equity", 0),
                    "proceeds_from_sale_of_treasury_stock": record.get("proceeds_from_sale_of_treasury_stock", 0),
                    "change_in_cash_and_cash_equivalents": record.get("change_in_cash_and_cash_equivalents", 0),
                    "change_in_exchange_rate": record.get("change_in_exchange_rate", 0),
                    "net_income": record.get("net_income", 0),
                }

                # Insert CashFlow node and create relationship
                session.run("""
                    MERGE (c:Company {id: $company_id})
                    MERGE (cf:CashFlow {
                        cash_flow_id: $cash_flow_id,
                        fiscal_date_ending: $fiscal_date_ending,
                        reported_currency: $reported_currency,
                        operating_cashflow: $operating_cashflow,
                        payments_for_operating_activities: $payments_for_operating_activities,
                        proceeds_from_operating_activities: $proceeds_from_operating_activities,
                        change_in_operating_liabilities: $change_in_operating_liabilities,
                        change_in_operating_assets: $change_in_operating_assets,
                        depreciation_depletion_and_amortization: $depreciation_depletion_and_amortization,
                        capital_expenditures: $capital_expenditures,
                        change_in_receivables: $change_in_receivables,
                        change_in_inventory: $change_in_inventory,
                        profit_loss: $profit_loss,
                        cashflow_from_investment: $cashflow_from_investment,
                        cashflow_from_financing: $cashflow_from_financing,
                        proceeds_from_repayments_of_short_term_debt: $proceeds_from_repayments_of_short_term_debt,
                        payments_for_repurchase_of_common_stock: $payments_for_repurchase_of_common_stock,
                        payments_for_repurchase_of_equity: $payments_for_repurchase_of_equity,
                        payments_for_repurchase_of_preferred_stock: $payments_for_repurchase_of_preferred_stock,
                        dividend_payout: $dividend_payout,
                        dividend_payout_common_stock: $dividend_payout_common_stock,
                        dividend_payout_preferred_stock: $dividend_payout_preferred_stock,
                        proceeds_from_issuance_of_common_stock: $proceeds_from_issuance_of_common_stock,
                        proceeds_from_issuance_of_long_term_debt: $proceeds_from_issuance_of_long_term_debt,
                        proceeds_from_issuance_of_preferred_stock: $proceeds_from_issuance_of_preferred_stock,
                        proceeds_from_repurchase_of_equity: $proceeds_from_repurchase_of_equity,
                        proceeds_from_sale_of_treasury_stock: $proceeds_from_sale_of_treasury_stock,
                        change_in_cash_and_cash_equivalents: $change_in_cash_and_cash_equivalents,
                        change_in_exchange_rate: $change_in_exchange_rate,
                        net_income: $net_income,
                        company_id: $company_id  // Adding company_id as property
                    
                    })
                    MERGE (c)-[:HAS_CASH_FLOW]->(cf)
                """, params)

        logging.info("Cash flow data successfully inserted into Neo4j.")

    except Exception as e:
        logging.error(f"Error inserting data into Neo4j: {e}")

# Main execution
if __name__ == "__main__":
    cash_flow_data = fetch_cash_flow_data()
    if cash_flow_data:
        insert_cash_flow_data_to_neo4j(cash_flow_data)
    else:
        logging.warning("No cash flow data to insert.")
