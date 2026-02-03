ALTER SYSTEM SET max_wal_size = '2GB';
ALTER SYSTEM SET synchronous_commit = off; 

DROP TABLE IF EXISTS invoice_items CASCADE;
DROP TABLE IF EXISTS invoices1 CASCADE;

CREATE TABLE invoices1 (
    invoice_number VARCHAR(100) PRIMARY KEY,
    invoice_type VARCHAR(50),
    issue_date DATE,
    due_date DATE,
    currency VARCHAR(10),
    vendor_name VARCHAR(500),
    vendor_taxid VARCHAR(50),
    buyer_name VARCHAR(500),
    buyer_taxid VARCHAR(50),
    net_total NUMERIC(15, 2),
    vat_total NUMERIC(15, 2),
    gross_total NUMERIC(15, 2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE invoice_items (
    id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(100) REFERENCES invoices1(invoice_number),
    description TEXT,
    quantity NUMERIC(15, 4),
    unit_price NUMERIC(15, 2),
    vat_rate NUMERIC(5, 2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_unique_item ON invoice_items(invoice_number, description, unit_price);

CREATE INDEX idx_items_invoice_num ON invoice_items(invoice_number);

CREATE UNLOGGED TABLE stg_headers AS SELECT * FROM invoices1 WITH NO DATA;
CREATE UNLOGGED TABLE stg_items AS SELECT * FROM invoice_items WITH NO DATA;
ALTER TABLE stg_items DROP COLUMN id;