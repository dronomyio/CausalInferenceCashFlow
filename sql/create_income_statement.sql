CREATE TABLE INCOME_STATEMENT (
    income_statement_id INT AUTO_INCREMENT PRIMARY KEY,        -- Unique ID for the income statement
    id INT NOT NULL,                                           -- company id
    fiscal_date_ending DATE NOT NULL,                          -- Fiscal date ending
    reported_currency VARCHAR(10) NOT NULL,                   -- Reported currency (e.g., USD)

    -- Revenue and Profit
    total_revenue BIGINT,                                      -- Total revenue
    gross_profit BIGINT,                                       -- Gross profit
    cost_of_revenue BIGINT,                                    -- Cost of revenue
    cost_of_goods_and_services_sold BIGINT,                   -- Cost of goods and services sold
    operating_income BIGINT,                                   -- Operating income
    net_income BIGINT,                                         -- Net income

    -- Expenses
    selling_general_and_administrative BIGINT,                -- Selling, general, and administrative expenses
    research_and_development BIGINT,                          -- Research and development expenses
    operating_expenses BIGINT,                                 -- Total operating expenses
    depreciation BIGINT,                                       -- Depreciation
    depreciation_and_amortization BIGINT,                     -- Depreciation and amortization
    income_tax_expense BIGINT,                                 -- Income tax expense

    -- Income and Earnings
    investment_income_net BIGINT,                              -- Net investment income
    net_interest_income BIGINT,                                -- Net interest income
    interest_income BIGINT,                                    -- Interest income
    interest_expense BIGINT,                                   -- Interest expense
    non_interest_income BIGINT,                                -- Non-interest income
    other_non_operating_income BIGINT,                        -- Other non-operating income
    income_before_tax BIGINT,                                  -- Income before tax
    net_income_from_continuing_operations BIGINT,             -- Net income from continuing operations
    comprehensive_income_net_of_tax BIGINT,                   -- Comprehensive income net of tax

    -- Key Performance Metrics
    ebit BIGINT,                                               -- Earnings before interest and taxes (EBIT)
    ebitda BIGINT,                                             -- Earnings before interest, taxes, depreciation, and amortization (EBITDA)

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,            -- Record creation timestamp
    FOREIGN KEY (id) REFERENCES Companies(id)

);
