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

# Fetch balance sheet data from MySQL
def fetch_balance_sheet_data():
    logging.info("Fetching balance sheet data from MySQL...")
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Query to fetch balance sheet data
        cursor.execute("""
            SELECT
                bs.*,
                c.id AS company_id  -- Ensure company_id is fetched
            FROM BALANCE_SHEET bs
            INNER JOIN Companies c ON c.id = bs.id  -- Adjust based on your table relationships
        """)

        data = cursor.fetchall()
        logging.info(f"Successfully fetched {len(data)} balance sheet records.")
        return data

    except mysql.connector.Error as e:
        logging.error(f"Error fetching data from MySQL: {e}")
        return []

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Insert balance sheet data into Neo4j
def insert_balance_sheet_data_to_neo4j(balance_sheet_data):
    logging.info("Preparing balance sheet data for Neo4j...")
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        with driver.session() as session:
            for record in balance_sheet_data:
                # Prepare parameters for Neo4j
                params = {
                    "company_id": record["company_id"],
                    "balance_sheet_id": record["balance_sheet_id"],
                    "fiscal_date_ending": record["fiscal_date_ending"],
                    "reported_currency": record["reported_currency"],
                    "total_assets": record.get("total_assets", 0),
                    "total_current_assets": record.get("total_current_assets", 0),
                    "cash_and_cash_equivalents": record.get("cash_and_cash_equivalents", 0),
                    "cash_and_short_term_investments": record.get("cash_and_short_term_investments", 0),
                    "inventory": record.get("inventory", 0),
                    "current_net_receivables": record.get("current_net_receivables", 0),
                    "total_non_current_assets": record.get("total_non_current_assets", 0),
                    "property_plant_equipment": record.get("property_plant_equipment", 0),
                    "accumulated_depreciation_amortization": record.get("accumulated_depreciation_amortization", 0),
                    "intangible_assets": record.get("intangible_assets", 0),
                    "intangible_assets_excluding_goodwill": record.get("intangible_assets_excluding_goodwill", 0),
                    "goodwill": record.get("goodwill", 0),
                    "investments": record.get("investments", 0),
                    "long_term_investments": record.get("long_term_investments", 0),
                    "short_term_investments": record.get("short_term_investments", 0),
                    "other_current_assets": record.get("other_current_assets", 0),
                    "other_non_current_assets": record.get("other_non_current_assets", 0),
                    "total_liabilities": record.get("total_liabilities", 0),
                    "total_current_liabilities": record.get("total_current_liabilities", 0),
                    "current_accounts_payable": record.get("current_accounts_payable", 0),
                    "deferred_revenue": record.get("deferred_revenue", 0),
                    "current_debt": record.get("current_debt", 0),
                    "short_term_debt": record.get("short_term_debt", 0),
                    "total_non_current_liabilities": record.get("total_non_current_liabilities", 0),
                    "capital_lease_obligations": record.get("capital_lease_obligations", 0),
                    "long_term_debt": record.get("long_term_debt", 0),
                    "current_long_term_debt": record.get("current_long_term_debt", 0),
                    "long_term_debt_noncurrent": record.get("long_term_debt_noncurrent", 0),
                    "short_long_term_debt_total": record.get("short_long_term_debt_total", 0),
                    "other_current_liabilities": record.get("other_current_liabilities", 0),
                    "other_non_current_liabilities": record.get("other_non_current_liabilities", 0),
                    "total_shareholder_equity": record.get("total_shareholder_equity", 0),
                    "treasury_stock": record.get("treasury_stock", 0),
                    "retained_earnings": record.get("retained_earnings", 0),
                    "common_stock": record.get("common_stock", 0),
                    "common_stock_shares_outstanding": record.get("common_stock_shares_outstanding", 0),
                }

                # Insert BalanceSheet node and relationship to Company node
                session.run("""
                    MATCH (c:Company {id: $company_id})
                    MERGE (bs:BalanceSheet {
                        balance_sheet_id: $balance_sheet_id,
                        fiscal_date_ending: $fiscal_date_ending,
                        reported_currency: $reported_currency,
                        total_assets: $total_assets,
                        total_current_assets: $total_current_assets,
                        cash_and_cash_equivalents: $cash_and_cash_equivalents,
                        cash_and_short_term_investments: $cash_and_short_term_investments,
                        inventory: $inventory,
                        current_net_receivables: $current_net_receivables,
                        total_non_current_assets: $total_non_current_assets,
                        property_plant_equipment: $property_plant_equipment,
                        accumulated_depreciation_amortization: $accumulated_depreciation_amortization,
                        intangible_assets: $intangible_assets,
                        intangible_assets_excluding_goodwill: $intangible_assets_excluding_goodwill,
                        goodwill: $goodwill,
                        investments: $investments,
                        long_term_investments: $long_term_investments,
                        short_term_investments: $short_term_investments,
                        other_current_assets: $other_current_assets,
                        other_non_current_assets: $other_non_current_assets,
                        total_liabilities: $total_liabilities,
                        total_current_liabilities: $total_current_liabilities,
                        current_accounts_payable: $current_accounts_payable,
                        deferred_revenue: $deferred_revenue,
                        current_debt: $current_debt,
                        short_term_debt: $short_term_debt,
                        total_non_current_liabilities: $total_non_current_liabilities,
                        capital_lease_obligations: $capital_lease_obligations,
                        long_term_debt: $long_term_debt,
                        current_long_term_debt: $current_long_term_debt,
                        long_term_debt_noncurrent: $long_term_debt_noncurrent,
                        short_long_term_debt_total: $short_long_term_debt_total,
                        other_current_liabilities: $other_current_liabilities,
                        other_non_current_liabilities: $other_non_current_liabilities,
                        total_shareholder_equity: $total_shareholder_equity,
                        treasury_stock: $treasury_stock,
                        retained_earnings: $retained_earnings,
                        common_stock: $common_stock,
                        common_stock_shares_outstanding: $common_stock_shares_outstanding,
                        company_id: $company_id  // Adding company_id as property
                    })
                    MERGE (c)-[:HAS_BALANCE_SHEET]->(bs)
                """, params)

        logging.info("Balance sheet data successfully inserted into Neo4j and linked to Company.")

    except Exception as e:
        logging.error(f"Error inserting data into Neo4j: {e}")

# Main execution
if __name__ == "__main__":
    balance_sheet_data = fetch_balance_sheet_data()
    if balance_sheet_data:
        insert_balance_sheet_data_to_neo4j(balance_sheet_data)
    else:
        logging.warning("No balance sheet data to insert.")

