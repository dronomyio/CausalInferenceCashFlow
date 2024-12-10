# Import necessary libraries
import time
from alpha_vantage.fundamentaldata import FundamentalData
import mysql.connector
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# MySQL connection details
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "root"
DB_NAME = "SPYStocks"

# Alpha Vantage API key
api_key = "JALM7ZS3V614IPA3"

# Initialize Alpha Vantage FundamentalData client
fd = FundamentalData(key=api_key, output_format="pandas")

# Helper function to handle None values
def safe_get(row, field, default=None):
    value = row.get(field)
    return default if value is None or value == 'None' else value

# Retry mechanism
def retry_operation(operation, max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            return operation()
        except mysql.connector.Error as e:
            if attempt < max_retries - 1:
                logging.warning(f"Operation failed, retrying in {delay} seconds... (Attempt {attempt + 1})")
                time.sleep(delay)
            else:
                logging.error(f"Operation failed after {max_retries} attempts: {e}")
                raise

# Fetch financial data with rate limiting
def fetch_financial_data(symbol):
    """Fetch financial data (income, balance sheet, cash flow) for a given company symbol."""
    try:
        logging.info(f"Fetching financial data for {symbol}...")
        income_statement, _ = fd.get_income_statement_annual(symbol=symbol)
        balance_sheet, _ = fd.get_balance_sheet_annual(symbol=symbol)
        cash_flow, _ = fd.get_cash_flow_annual(symbol=symbol)

        # Check if DataFrames are empty
        if income_statement.empty:
            logging.warning(f"Income statement is empty for {symbol}")
            income_statement = None
        if balance_sheet.empty:
            logging.warning(f"Balance sheet is empty for {symbol}")
            balance_sheet = None
        if cash_flow.empty:
            logging.warning(f"Cash flow is empty for {symbol}")
            cash_flow = None

        return income_statement, balance_sheet, cash_flow
    except Exception as e:
        logging.error(f"Error fetching financial data for {symbol}: {e}")
        return None, None, None

# Insert financial data into MySQL
def insert_financial_data(symbol, company_id, income_statement, balance_sheet, cash_flow):
#def insert_financial_data(company_id, income_statement):
    """Insert financial data into the INCOME_STATEMENT table."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        cursor = conn.cursor()

        if cash_flow is not None:
            for _, row in cash_flow.iterrows():
                params = (
                    company_id,
                    safe_get(row, 'fiscalDateEnding'),
                    safe_get(row, 'reportedCurrency'),
                    safe_get(row, 'operatingCashflow', 0),
                    safe_get(row, 'paymentsForOperatingActivities', 0),
                    safe_get(row, 'proceedsFromOperatingActivities', 0),
                    safe_get(row, 'changeInOperatingLiabilities', 0),
                    safe_get(row, 'changeInOperatingAssets', 0),
                    safe_get(row, 'depreciationDepletionAndAmortization', 0),
                    safe_get(row, 'capitalExpenditures', 0),
                    safe_get(row, 'changeInReceivables', 0),
                    safe_get(row, 'changeInInventory', 0),
                    safe_get(row, 'profitLoss', 0),
                    safe_get(row, 'cashflowFromInvestment', 0),
                    safe_get(row, 'cashflowFromFinancing', 0),
                    safe_get(row, 'proceedsFromRepaymentsOfShortTermDebt', 0),
                    safe_get(row, 'paymentsForRepurchaseOfCommonStock', 0),
                    safe_get(row, 'paymentsForRepurchaseOfEquity', 0),
                    safe_get(row, 'paymentsForRepurchaseOfPreferredStock', 0),
                    safe_get(row, 'dividendPayout', 0),
                    safe_get(row, 'dividendPayoutCommonStock', 0),
                    safe_get(row, 'dividendPayoutPreferredStock', 0),
                    safe_get(row, 'proceedsFromIssuanceOfCommonStock', 0),
                    safe_get(row, 'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet', 0),
                    safe_get(row, 'proceedsFromIssuanceOfPreferredStock', 0),
                    safe_get(row, 'proceedsFromRepurchaseOfEquity', 0),
                    safe_get(row, 'proceedsFromSaleOfTreasuryStock', 0),
                    safe_get(row, 'changeInCashAndCashEquivalents', 0),
                    safe_get(row, 'changeInExchangeRate', 0),
                    safe_get(row, 'netIncome', 0)
                )

                # Debug: Validate the parameters
                logging.debug(f"CASH_FLOW params length: {len(params)}; params: {params}")

                # Ensure the number of placeholders matches the params length
                if len(params) != 30:  # Expected number of parameters for CASH_FLOW
                    logging.error(f"Parameter count mismatch for CASH_FLOW. Expected 31, got {len(params)}. Params: {params}")
                    continue

                cursor.execute(
                    """
                    INSERT INTO CASH_FLOW (
                        id, fiscal_date_ending, reported_currency, operating_cashflow,
                        payments_for_operating_activities, proceeds_from_operating_activities, 
                        change_in_operating_liabilities, change_in_operating_assets, 
                        depreciation_depletion_and_amortization, capital_expenditures, 
                        change_in_receivables, change_in_inventory, profit_loss, cashflow_from_investment, 
                        cashflow_from_financing, proceeds_from_repayments_of_short_term_debt, 
                        payments_for_repurchase_of_common_stock, payments_for_repurchase_of_equity, 
                        payments_for_repurchase_of_preferred_stock, dividend_payout, 
                        dividend_payout_common_stock, dividend_payout_preferred_stock, 
                        proceeds_from_issuance_of_common_stock, proceeds_from_issuance_of_long_term_debt, 
                        proceeds_from_issuance_of_preferred_stock, proceeds_from_repurchase_of_equity, 
                        proceeds_from_sale_of_treasury_stock, change_in_cash_and_cash_equivalents, 
                        change_in_exchange_rate, net_income
                    ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params
                )
                logging.info(f"Inserted CASH_FLOW for {symbol}")

        if balance_sheet is not None:
            for _, row in balance_sheet.iterrows():
                params = (
                    company_id,
                    safe_get(row, 'fiscalDateEnding'),
                    safe_get(row, 'reportedCurrency'),
                    safe_get(row, 'totalAssets', 0),
                    safe_get(row, 'totalCurrentAssets', 0),
                    safe_get(row, 'cashAndCashEquivalentsAtCarryingValue', 0),
                    safe_get(row, 'cashAndShortTermInvestments', 0),
                    safe_get(row, 'inventory', 0),
                    safe_get(row, 'currentNetReceivables', 0),
                    safe_get(row, 'totalNonCurrentAssets', 0),
                    safe_get(row, 'propertyPlantEquipment', 0),
                    safe_get(row, 'accumulatedDepreciationAmortizationPPE', 0),
                    safe_get(row, 'intangibleAssets', 0),
                    safe_get(row, 'intangibleAssetsExcludingGoodwill', 0),
                    safe_get(row, 'goodwill', 0),
                    safe_get(row, 'investments', 0),
                    safe_get(row, 'longTermInvestments', 0),
                    safe_get(row, 'shortTermInvestments', 0),
                    safe_get(row, 'otherCurrentAssets', 0),
                    safe_get(row, 'otherNonCurrentAssets', 0),
                    safe_get(row, 'totalLiabilities', 0),
                    safe_get(row, 'totalCurrentLiabilities', 0),
                    safe_get(row, 'currentAccountsPayable', 0),
                    safe_get(row, 'deferredRevenue', 0),
                    safe_get(row, 'currentDebt', 0),
                    safe_get(row, 'shortTermDebt', 0),
                    safe_get(row, 'totalNonCurrentLiabilities', 0),
                    safe_get(row, 'capitalLeaseObligations', 0),
                    safe_get(row, 'longTermDebt', 0),
                    safe_get(row, 'currentLongTermDebt', 0),
                    safe_get(row, 'longTermDebtNoncurrent', 0),
                    safe_get(row, 'shortLongTermDebtTotal', 0),
                    safe_get(row, 'otherCurrentLiabilities', 0),
                    safe_get(row, 'otherNonCurrentLiabilities', 0),
                    safe_get(row, 'totalShareholderEquity', 0),
                    safe_get(row, 'treasuryStock', 0),
                    safe_get(row, 'retainedEarnings', 0),
                    safe_get(row, 'commonStock', 0),
                    safe_get(row, 'commonStockSharesOutstanding', 0)
                )

                # Validate parameter length
                #if len(params) != 40:  # Expected number of parameters for BALANCE_SHEET
                if len(params) != 39:  # Expected number of parameters for BALANCE_SHEET
                    logging.error(f"Parameter count mismatch for BALANCE_SHEET. Expected 40, got {len(params)}. Params: {params}")
                    continue

                cursor.execute(
                    """
                    INSERT INTO BALANCE_SHEET (
                        id, fiscal_date_ending, reported_currency, total_assets, total_current_assets, 
                        cash_and_cash_equivalents, cash_and_short_term_investments, inventory, 
                        current_net_receivables, total_non_current_assets, property_plant_equipment, 
                        accumulated_depreciation_amortization, intangible_assets, intangible_assets_excluding_goodwill,
                        goodwill, investments, long_term_investments, short_term_investments, other_current_assets,
                        other_non_current_assets, total_liabilities, total_current_liabilities, current_accounts_payable,
                        deferred_revenue, current_debt, short_term_debt, total_non_current_liabilities, capital_lease_obligations,
                        long_term_debt, current_long_term_debt, long_term_debt_noncurrent, short_long_term_debt_total,
                        other_current_liabilities, other_non_current_liabilities, total_shareholder_equity, treasury_stock,
                        retained_earnings, common_stock, common_stock_shares_outstanding
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params
                )
                logging.info(f"Inserted BALANCE_SHEET for {symbol}")

        # Insert Income Statement
        if income_statement is not None:
            for _, row in income_statement.iterrows():
                params = (
                    company_id,
                    row['fiscalDateEnding'],
                    row['reportedCurrency'],
                    safe_get(row, 'totalRevenue', 0),
                    safe_get(row, 'grossProfit', 0),
                    safe_get(row, 'costOfRevenue', 0),
                    safe_get(row, 'costofGoodsAndServicesSold', 0),
                    safe_get(row, 'operatingIncome', 0),
                    safe_get(row, 'netIncome', 0),
                    safe_get(row, 'sellingGeneralAndAdministrative', 0),
                    safe_get(row, 'researchAndDevelopment', 0),
                    safe_get(row, 'operatingExpenses', 0),
                    safe_get(row, 'depreciation', 0),
                    safe_get(row, 'depreciationAndAmortization', 0),
                    safe_get(row, 'incomeBeforeTax', 0),
                    safe_get(row, 'incomeTaxExpense', 0),
                    safe_get(row, 'investmentIncomeNet', 0),
                    safe_get(row, 'netInterestIncome', 0),
                    safe_get(row, 'interestIncome', 0),
                    safe_get(row, 'interestExpense', 0),
                    safe_get(row, 'nonInterestIncome', 0),
                    safe_get(row, 'otherNonOperatingIncome', 0),
                    safe_get(row, 'netIncomeFromContinuingOperations', 0),
                    safe_get(row, 'comprehensiveIncomeNetOfTax', 0),
                    safe_get(row, 'ebit', 0),
                    safe_get(row, 'ebitda', 0)
                )
                cursor.execute(
                    """
                    INSERT INTO INCOME_STATEMENT (
                        id, fiscal_date_ending, reported_currency, total_revenue, gross_profit, 
                        cost_of_revenue, cost_of_goods_and_services_sold, operating_income, net_income, 
                        selling_general_and_administrative, research_and_development, operating_expenses, 
                        depreciation, depreciation_and_amortization, income_before_tax, income_tax_expense, 
                        investment_income_net, net_interest_income, interest_income, interest_expense, 
                        non_interest_income, other_non_operating_income, net_income_from_continuing_operations, 
                        comprehensive_income_net_of_tax, ebit, ebitda
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    params
                )
                logging.info(f"Inserted INCOME_STATEMENT for company_id {company_id}")

        conn.commit()
        logging.info(f"Successfully inserted financial data for company_id {company_id}.")
    except mysql.connector.Error as e:
        logging.error(f"Database error for company_id {company_id}: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Fetch or insert company and retrieve company_id
def get_company_id(symbol, company_name):
    """Fetch or insert company into MySQL and return its ID."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        cursor = conn.cursor()

        # Check if the company already exists
        cursor.execute("SELECT id FROM Companies WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()

        if result:
            company_id = result[0]
            logging.info(f"Found existing company ID for {symbol}: {company_id}")

        return company_id
    except mysql.connector.Error as e:
        logging.error(f"Database error while fetching/inserting company: {e}")
        raise
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Main execution for a set of stocks
if __name__ == "__main__":
    #some example stocks
    stocks = [
        {"symbol": "AVB", "company_name": "AVB"},
        {"symbol": "CME", "company_name": "CME"},
        {"symbol": "EQR", "company_name": "EQR"},
        {"symbol": "BEN", "company_name": "BEN"},
        {"symbol": "HST", "company_name": "HST"},
        {"symbol": "NTRS", "company_name": "NTRS"},
        {"symbol": "PLD", "company_name": "PLD"},
        {"symbol": "STT", "company_name": "STT"},
        {"symbol": "USB", "company_name": "USB"},
        {"symbol": "VTR", "company_name": "VTR"},
        {"symbol": "AEE", "company_name": "AEE"},
        {"symbol": "AEP", "company_name": "AEP"},
        {"symbol": "CMS", "company_name": "CMS"},
        {"symbol": "DTE", "company_name": "DTE"},
        {"symbol": "DUK", "company_name": "DUK"},
        {"symbol": "ETR", "company_name": "ETR"},
        {"symbol": "PNW", "company_name": "PNW"},
        {"symbol": "PPL", "company_name": "PPL"},
        {"symbol": "WEC", "company_name": "WEC"},
        {"symbol": "XEL", "company_name": "XEL"},
        {"symbol": "A", "company_name": "A"},
        {"symbol": "AKAM", "company_name": "AKAM"},
        {"symbol": "ADSK", "company_name": "ADSK"},
        {"symbol": "CSCO", "company_name": "CSCO"},
        {"symbol": "CTSH", "company_name": "CTSH"},
        {"symbol": "EBAY", "company_name": "EBAY"},
        {"symbol": "MCHP", "company_name": "MCHP"},
        {"symbol": "PAYX", "company_name": "PAYX"},
        {"symbol": "TXN", "company_name": "TXN"},
        {"symbol": "VRSN", "company_name": "VRSN"},
        {"symbol": "ABT", "company_name": "ABT"},
        {"symbol": "BDX", "company_name": "BDX"},
        {"symbol": "BIIB", "company_name": "BIIB"},
        {"symbol": "BMY", "company_name": "BMY"},
        {"symbol": "GILD", "company_name": "GILD"},
        {"symbol": "HUM", "company_name": "HUM"},
        {"symbol": "JNJ", "company_name": "JNJ"},
        {"symbol": "LH", "company_name": "LH"},
        {"symbol": "MDT", "company_name": "MDT"},
        {"symbol": "DGX", "company_name": "DGX"}
    ]

    for stock in stocks:
        symbol = stock["symbol"]
        company_name = stock["company_name"]

        try:
            # Get company ID
            company_id = get_company_id(symbol, company_name)

            # Fetch and insert financial data
            income_statement, balance_sheet, cash_flow = fetch_financial_data(symbol)
            if income_statement is not None or balance_sheet is not None or cash_flow is not None:
               retry_operation(
                   lambda: insert_financial_data(symbol, company_id, income_statement, balance_sheet, cash_flow)
               )
        except Exception as e:
            logging.error(f"Error processing {company_name} ({symbol}): {e}")
