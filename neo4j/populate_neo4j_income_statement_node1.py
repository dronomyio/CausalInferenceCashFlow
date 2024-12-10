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

# Fetch INCOME_STATEMENT data from MySQL
def fetch_income_statement_data():
    logging.info("Fetching income statement data from MySQL...")
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Query to fetch income statement data
        cursor.execute("""
            SELECT
                inc.*,
                c.id AS company_id
            FROM INCOME_STATEMENT inc
            INNER JOIN Companies c ON inc.id = c.id
        """)

        data = cursor.fetchall()
        logging.info(f"Successfully fetched {len(data)} income statement records.")
        return data

    except mysql.connector.Error as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        return []

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Insert INCOME_STATEMENT data into Neo4j
def insert_income_statement_data_to_neo4j(income_statement_data):
    logging.info("Preparing income statement data for Neo4j...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        with driver.session() as session:
            for record in income_statement_data:
                # Prepare parameters for Neo4j
                params = {
                    "company_id": record["company_id"],
                    "income_statement_id": record["income_statement_id"],
                    "fiscal_date_ending": record["fiscal_date_ending"],
                    "reported_currency": record["reported_currency"],
                    "total_revenue": record.get("total_revenue", 0),
                    "gross_profit": record.get("gross_profit", 0),
                    "cost_of_revenue": record.get("cost_of_revenue", 0),
                    "cost_of_goods_and_services_sold": record.get("cost_of_goods_and_services_sold", 0),
                    "operating_income": record.get("operating_income", 0),
                    "net_income": record.get("net_income", 0),
                    "selling_general_and_administrative": record.get("selling_general_and_administrative", 0),
                    "research_and_development": record.get("research_and_development", 0),
                    "operating_expenses": record.get("operating_expenses", 0),
                    "depreciation": record.get("depreciation", 0),
                    "depreciation_and_amortization": record.get("depreciation_and_amortization", 0),
                    "income_tax_expense": record.get("income_tax_expense", 0),
                    "investment_income_net": record.get("investment_income_net", 0),
                    "net_interest_income": record.get("net_interest_income", 0),
                    "interest_income": record.get("interest_income", 0),
                    "interest_expense": record.get("interest_expense", 0),
                    "non_interest_income": record.get("non_interest_income", 0),
                    "other_non_operating_income": record.get("other_non_operating_income", 0),
                    "income_before_tax": record.get("income_before_tax", 0),
                    "net_income_from_continuing_operations": record.get("net_income_from_continuing_operations", 0),
                    "comprehensive_income_net_of_tax": record.get("comprehensive_income_net_of_tax", 0),
                    "ebit": record.get("ebit", 0),
                    "ebitda": record.get("ebitda", 0),
                }

                # Insert data and create relationships in Neo4j
                session.run("""
                    MERGE (c:Company {id: $company_id})
                    MERGE (is:IncomeStatement {
                        income_statement_id: $income_statement_id,
                        fiscal_date_ending: $fiscal_date_ending,
                        reported_currency: $reported_currency,
                        total_revenue: $total_revenue,
                        gross_profit: $gross_profit,
                        cost_of_revenue: $cost_of_revenue,
                        cost_of_goods_and_services_sold: $cost_of_goods_and_services_sold,
                        operating_income: $operating_income,
                        net_income: $net_income,
                        selling_general_and_administrative: $selling_general_and_administrative,
                        research_and_development: $research_and_development,
                        operating_expenses: $operating_expenses,
                        depreciation: $depreciation,
                        depreciation_and_amortization: $depreciation_and_amortization,
                        income_tax_expense: $income_tax_expense,
                        investment_income_net: $investment_income_net,
                        net_interest_income: $net_interest_income,
                        interest_income: $interest_income,
                        interest_expense: $interest_expense,
                        non_interest_income: $non_interest_income,
                        other_non_operating_income: $other_non_operating_income,
                        income_before_tax: $income_before_tax,
                        net_income_from_continuing_operations: $net_income_from_continuing_operations,
                        comprehensive_income_net_of_tax: $comprehensive_income_net_of_tax,
                        ebit: $ebit,
                        ebitda: $ebitda,
                        company_id: $company_id  // Adding company_id as property
                    })
                    MERGE (c)-[:HAS_INCOME_STATEMENT]->(is)
                """, params)

        logging.info("Income statement data and relationships successfully inserted into Neo4j.")

    except Exception as e:
        logging.error(f"Error inserting data into Neo4j: {e}")

# Main execution
if __name__ == "__main__":
    income_statement_data = fetch_income_statement_data()
    if income_statement_data:
        insert_income_statement_data_to_neo4j(income_statement_data)
    else:
        logging.warning("No income statement data to insert.")

