CREATE TABLE BALANCE_SHEET (
    balance_sheet_id INT AUTO_INCREMENT PRIMARY KEY,   /* Unique ID for each record */
    id INT NOT NULL,                           /* Foreign key reference to Companies table */
    fiscal_date_ending DATE NOT NULL,                 /* Fiscal date ending */
    reported_currency VARCHAR(10) NOT NULL,           /* Reported currency (e.g., USD) */

    /* Assets */
    total_assets BIGINT,                              /* Total assets */
    total_current_assets BIGINT,                     /* Total current assets */
    cash_and_cash_equivalents BIGINT,                /* Cash and cash equivalents at carrying value */
    cash_and_short_term_investments BIGINT,          /* Cash and short-term investments */
    inventory BIGINT,                                 /* Inventory */
    current_net_receivables BIGINT,                  /* Current net receivables */
    total_non_current_assets BIGINT,                 /* Total non-current assets */
    property_plant_equipment BIGINT,                 /* Property, plant, and equipment */
    accumulated_depreciation_amortization BIGINT,    /* Accumulated depreciation and amortization */
    intangible_assets BIGINT,                        /* Intangible assets */
    intangible_assets_excluding_goodwill BIGINT,     /* Intangible assets excluding goodwill */
    goodwill BIGINT,                                  /* Goodwill */
    investments BIGINT,                               /* Investments */
    long_term_investments BIGINT,                    /* Long-term investments */
    short_term_investments BIGINT,                   /* Short-term investments */
    other_current_assets BIGINT,                     /* Other current assets */
    other_non_current_assets BIGINT,                 /* Other non-current assets */

    /* Liabilities */
    total_liabilities BIGINT,                        /* Total liabilities */
    total_current_liabilities BIGINT,                /* Total current liabilities */
    current_accounts_payable BIGINT,                 /* Current accounts payable */
    deferred_revenue BIGINT,                         /* Deferred revenue */
    current_debt BIGINT,                             /* Current debt */
    short_term_debt BIGINT,                          /* Short-term debt */
    total_non_current_liabilities BIGINT,            /* Total non-current liabilities */
    capital_lease_obligations BIGINT,                /* Capital lease obligations */
    long_term_debt BIGINT,                           /* Long-term debt */
    current_long_term_debt BIGINT,                   /* Current portion of long-term debt */
    long_term_debt_noncurrent BIGINT,                /* Long-term debt (non-current) */
    short_long_term_debt_total BIGINT,               /* Total of short and long-term debt */
    other_current_liabilities BIGINT,                /* Other current liabilities */
    other_non_current_liabilities BIGINT,            /* Other non-current liabilities */

    /* Shareholder Equity */
    total_shareholder_equity BIGINT,                 /* Total shareholder equity */
    treasury_stock BIGINT,                           /* Treasury stock */
    retained_earnings BIGINT,                        /* Retained earnings */
    common_stock BIGINT,                             /* Common stock */
    common_stock_shares_outstanding BIGINT,          /* Common stock shares outstanding */

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  /* Record creation timestamp */
    FOREIGN KEY (id) REFERENCES Companies(id) /* Foreign key constraint */
);
