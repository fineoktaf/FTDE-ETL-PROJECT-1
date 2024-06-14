SELECT
  DISTINCT EMPLOYEEID,
  TRIM(LASTNAME) AS LASTNAME,
  TRIM(FIRSTNAME) AS FIRSTNAME,
  TRIM(TITLE) AS TITLE,
  TRIM(TITLEOFCOURTESY) AS TITLEOFCOURTESY,
  TRIM(BIRTHDATE) AS BIRTHDATE,
  TRIM(HIREDATE) AS HIREDATE,
  TRIM(ADDRESS) AS ADDRESS,
  TRIM(CITY) AS CITY,
  TRIM(REGION) AS REGION,
  TRIM(POSTALCODE) AS POSTALCODE,
  TRIM(COUNTRY) AS COUNTRY,
  REGEXP_REPLACE(TRIM(HOMEPHONE), '[^0-9]', '') AS HOMEPHONE,
  TRIM(EXTENSION) AS EXTENSION,
  TO_BINARY(PHOTO, 'BASE64') AS PHOTO,
  TRIM(NOTES) AS NOTES,
  COALESCE(TRIM(REPORTSTO), '0') AS REPORTSTO,
  TRIM(PHOTOPATH) AS PHOTOPATH
FROM
    {{ source('sources', 'employees') }}
WHERE EMPLOYEEID IS NOT NULL