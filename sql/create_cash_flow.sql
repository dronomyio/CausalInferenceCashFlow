CREATE TABLE CASH_FLOW (
    cash_flow_id INT AUTO_INCREMENT PRIMARY KEY,                -- Unique ID for the cash flow record
    id INT NOT NULL,                                           -- company id
    fiscal_date_ending DATE NOT NULL,                          -- Fiscal date ending
    reported_currency VARCHAR(10) NOT NULL,                   -- Reported currency (e.g., USD)

    -- Operating Activities
    operating_cashflow BIGINT,                                 -- Operating cash flow
    payments_for_operating_activities BIGINT,                  -- Payments for operating activities
    proceeds_from_operating_activities BIGINT,                 -- Proceeds from operating activities
    change_in_operating_liabilities BIGINT,                    -- Change in operating liabilities
    change_in_operating_assets BIGINT,                         -- Change in operating assets
    depreciation_depletion_and_amortization BIGINT,            -- Depreciation, depletion, and amortization
    capital_expenditures BIGINT,                               -- Capital expenditures
    change_in_receivables BIGINT,                              -- Change in receivables
    change_in_inventory BIGINT,                                -- Change in inventory
    profit_loss BIGINT,                                        -- Profit or loss

    -- Investing Activities
    cashflow_from_investment BIGINT,                           -- Cash flow from investment

    -- Financing Activities
    cashflow_from_financing BIGINT,                            -- Cash flow from financing
    proceeds_from_repayments_of_short_term_debt BIGINT,        -- Proceeds from repayments of short-term debt
    payments_for_repurchase_of_common_stock BIGINT,            -- Payments for repurchase of common stock
    payments_for_repurchase_of_equity BIGINT,                  -- Payments for repurchase of equity
    payments_for_repurchase_of_preferred_stock BIGINT,         -- Payments for repurchase of preferred stock
    dividend_payout BIGINT,                                    -- Dividend payout
    dividend_payout_common_stock BIGINT,                       -- Dividend payout for common stock
    dividend_payout_preferred_stock BIGINT,                    -- Dividend payout for preferred stock
    proceeds_from_issuance_of_common_stock BIGINT,             -- Proceeds from issuance of common stock
    proceeds_from_issuance_of_long_term_debt BIGINT,           -- Proceeds from issuance of long-term debt and capital securities
    proceeds_from_issuance_of_preferred_stock BIGINT,          -- Proceeds from issuance of preferred stock
    proceeds_from_repurchase_of_equity BIGINT,                 -- Proceeds from repurchase of equity
    proceeds_from_sale_of_treasury_stock BIGINT,               -- Proceeds from sale of treasury stock

    -- Other Fields
    change_in_cash_and_cash_equivalents BIGINT,                -- Change in cash and cash equivalents
    change_in_exchange_rate BIGINT,                            -- Change in exchange rate
    net_income BIGINT,                                         -- Net income

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,             -- Record creation timestamp
    FOREIGN KEY (id) REFERENCES Companies(id)
);
