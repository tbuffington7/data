CREATE OR REPLACE function nfpa_region(state varchar) returns varchar(2) AS $$
BEGIN
  IF upper(state) in ('AL', 'AR', 'DC', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'OK', 'SC', 'TN', 'TX', 'VA', 'WV') THEN
    RETURN 'South';
  ELSIF upper(state) in ('AK', 'AZ', 'CA', 'CO', 'HI', 'ID', 'MT', 'NM', 'NV', 'OR', 'UT', 'WA', 'WY') THEN
    RETURN 'West';
  ELSIF upper(state) in ('CT', 'MA', 'ME', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT') THEN
    RETURN 'Northeast';
  ELSIF upper(state) in ('IA', 'IL', 'IN', 'KS', 'MI', 'MN', 'MO', 'ND', 'NE', 'OH', 'SD', 'WI') THEN
    RETURN 'Midwest';
  ELSE RETURN '';
  END IF;
END;
$$ LANGUAGE plpgsql;
