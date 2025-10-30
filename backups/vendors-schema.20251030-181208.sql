CREATE TABLE IF NOT EXISTS "vendors"(
  id INT,
  business_name TEXT,
  category TEXT,
  service TEXT,
  contact_name TEXT,
  phone TEXT,
  email TEXT,
  website TEXT,
  address TEXT,
  notes TEXT,
  created_at TEXT,
  updated_at TEXT,
  computed_keywords TEXT,
  ckw_locked INT,
  ckw_version TEXT,
  ckw_manual_extra TEXT
, keywords TEXT, ckw TEXT DEFAULT '');
CREATE INDEX idx_vendors_bus_lower ON vendors(lower(business_name));
CREATE INDEX idx_vendors_phone ON vendors(phone);
CREATE INDEX idx_vendors_ckw ON vendors(computed_keywords);
CREATE INDEX idx_vendors_id            ON vendors(id);
idx_vendors_bus_lower|CREATE INDEX idx_vendors_bus_lower ON vendors(lower(business_name))
idx_vendors_ckw|CREATE INDEX idx_vendors_ckw ON vendors(computed_keywords)
idx_vendors_id|CREATE INDEX idx_vendors_id            ON vendors(id)
idx_vendors_phone|CREATE INDEX idx_vendors_phone ON vendors(phone)
