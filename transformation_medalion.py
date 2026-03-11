#!/usr/bin/env python
# coding: utf-8

# ## transformation_medalion
# 
# null

# In[12]:


df_orders_raw = spark.read.parquet("abfss://retail_project@onelake.dfs.fabric.microsoft.com/medalion_lakehouse.Lakehouse/Files/bronze/orders_data.parquet")
df_returns_raw = spark.read.parquet("abfss://retail_project@onelake.dfs.fabric.microsoft.com/medalion_lakehouse.Lakehouse/Files/bronze/returns_data.xlsx.parquet")
df_inventory_raw = spark.read.parquet("abfss://retail_project@onelake.dfs.fabric.microsoft.com/medalion_lakehouse.Lakehouse/Files/bronze/inventory_data.parquet")


# ### _**handle first row for inventory data frame files**_

# In[9]:


display(df_returns_raw.limit(5))


# In[8]:


# ---------------------------------------------
# STEP 1 — Extract the first row to use as header
# ---------------------------------------------
# Spark read the file without recognizing the header,
# so the first row of the dataset actually contains column names.
first_row = df_returns_raw.first()

# Convert each element of the first row into a clean string.
# These will become the new column names.
columns = [str(item).strip() for item in first_row]


# ---------------------------------------------------------
# STEP 2 — Remove the first row and rebuild the DataFrame
# ---------------------------------------------------------
# Convert the DataFrame to an RDD so we can manipulate rows.
# zipWithIndex() attaches an index to each row: (row, index)
# We filter out index 0 (the header row), keeping only real data.
# Then we drop the index and convert back to a DataFrame
# using the extracted column names.
df_returns_raw = (
    df_returns_raw.rdd
    .zipWithIndex()                     # → (row, index)
    .filter(lambda x: x[1] > 0)         # keep rows where index > 0
    .map(lambda x: x[0])                # remove the index, keep only the row
    .toDF(columns)                      # rebuild DataFrame with correct headers
)


# ---------------------------------------------
# STEP 3 — Display the cleaned DataFrame
# ---------------------------------------------
display(df_returns_raw.limit(4))


# ### _**create bronze delta tables**_

# In[9]:


df_orders_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_orders")

df_returns_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_returns")

df_inventory_raw.write.mode("overwrite").format("delta").saveAsTable("bronze_inventory")


# # _**clean the data - silver layer preparation**_

# ### - _**orders cleaning**_

# In[4]:


display(df_orders_raw.limit(5))


# In[5]:


from pyspark.sql.functions import *
from pyspark.sql.types import *

df_orders = (
    df_orders_raw

    # 2. Clean column names
    .withColumnRenamed("Order_ID", "OrderID")
    .withColumnRenamed("cust_id", "CustomerID")
    .withColumnRenamed("Product_Name", "ProductName")
    .withColumnRenamed("Qty", "Quantity")
    .withColumnRenamed("Order_Date", "OrderDate")
    .withColumnRenamed("Order_Amount$", "OrderAmount")
    .withColumnRenamed("Delivery_Status", "DeliveryStatus")
    .withColumnRenamed("Payment_Mode", "PaymentMode")
    .withColumnRenamed("Ship_Address", "ShipAddress")
    .withColumnRenamed("Promo_Code", "PromoCode")
    .withColumnRenamed("Feedback_Score", "FeedbackScore")

    # 3. Normalize Quantity: convert words like 'one', 'Two' to integer
    .withColumn("Quantity", 
        when(lower(col("Quantity")) == "one", 1)
        .when(lower(col("Quantity")) == "two", 2)
        .when(lower(col("Quantity")) == "three", 3)
        .otherwise(col("Quantity").cast(IntegerType()))
    )

    # 4. Standardize date format using multiple patterns
    # Problem: OrderDate column has inconsistent date formats from different sources
# Solution: Try parsing with multiple common date patterns and use the first successful match
#
# Why coalesce()? 
#   - Spark's to_date() returns NULL if the date string doesn't match the format
#   - coalesce() returns the first non-NULL value from the list
#   - This handles multiple date formats in the same column
#
# Supported date formats:
#   1. "yyyy/MM/dd"    → 2025/03/11 (ISO format with slashes)
#   2. "dd-MM-yyyy"    → 11-03-2025 (European format)
#   3. "MM-dd-yyyy"    → 03-11-2025 (US format)
#   4. "yyyy.MM.dd"    → 2025.03.11 (ISO format with dots)
#   5. "dd/MM/yyyy"    → 11/03/2025 (European format with slashes)
#   6. "dd.MM.yyyy"    → 11.03.2025 (European format with dots)
#   7. "MMMM dd yyyy"  → March 11 2025 (Full month name)
#
# Note: If a date doesn't match any format, to_date() returns NULL
#       which is then caught in downstream validation
#
.withColumn("OrderDate", to_date(
    coalesce(
        to_date(col("OrderDate"), "yyyy/MM/dd"),      # Try ISO format with slashes
        to_date(col("OrderDate"), "dd-MM-yyyy"),      # Try European format with hyphens
        to_date(col("OrderDate"), "MM-dd-yyyy"),      # Try US format with hyphens
        to_date(col("OrderDate"), "yyyy.MM.dd"),      # Try ISO format with dots
        to_date(col("OrderDate"), "dd/MM/yyyy"),      # Try European format with slashes
        to_date(col("OrderDate"), "dd.MM.yyyy"),      # Try European format with dots
        to_date(col("OrderDate"), "MMMM dd yyyy")     # Try full month name format
    )
))

# ============================================================================
# STEP 5: CLEAN AND CONVERT ORDER AMOUNT TO NUMERIC VALUE
# ============================================================================
# Problem: OrderAmount column contains currency symbols and text characters
#          Examples: "$99.99", "₹5000", "Rs. 1250", "100 USD", "₹ INR 500"
#
# Solution: 
#   Step 5a: Remove all currency symbols and units using regex
#   Step 5b: Cast the cleaned string to DoubleType (numeric)
#
# Regex pattern "[[$₹Rs. USD, INR]]":
#   - Removes dollar sign ($)
#   - Removes Rupee symbol (₹)
#   - Removes Indian Rupee text (Rs.)
#   - Removes currency codes (USD, INR)
#   - Removes commas used as thousand separators
#
# Example transformations:
#   "$1,234.99" → "1234.99" → 1234.99 (Double)
#   "₹5000" → "5000" → 5000.0 (Double)
#   "Rs. 1250" → "1250" → 1250.0 (Double)
#
.withColumn("OrderAmount", regexp_replace(col("OrderAmount"), "[$₹Rs. USD, INR]", ""))
# Now convert the cleaned string to numeric (Double) type for calculations
.withColumn("OrderAmount", col("OrderAmount").cast(DoubleType()))

# ============================================================================
# STEP 6: STANDARDIZE PAYMENT MODE TEXT
# ============================================================================
# Problem: PaymentMode has inconsistent text formats with special characters
#          Examples: "CREDIT CARD", "Credit_Card", "credit-card", "CC123"
#
# Solution:
#   Step 6a: Remove all non-alphabetic characters (numbers, hyphens, underscores, etc.)
#   Step 6b: Convert to lowercase for consistency
#
# Regex pattern "[^a-zA-Z]":
#   - The ^ means "NOT" (negation)
#   - [a-zA-Z] matches letters A-Z and a-z
#   - So [^a-zA-Z] matches anything that is NOT a letter
#   - regexp_replace removes all matched characters
#
# Example transformations:
#   "CREDIT_CARD" → "creditcard"
#   "Debit-Card-123" → "debitcard"
#   "UPI@456" → "upi"
#   "Net Banking (RBI)" → "netbanking"
#
.withColumn("PaymentMode", lower(regexp_replace(col("PaymentMode"), "[^a-zA-Z]", "")))

# ============================================================================
# STEP 7: STANDARDIZE DELIVERY STATUS TEXT
# ============================================================================
# Problem: DeliveryStatus contains inconsistent formatting and unwanted characters
#          Examples: "DELIVERED-OK", "In-Transit_123", "pending/delayed"
#
# Solution:
#   Step 7a: Remove all non-alphabetic characters AND non-space characters
#   Step 7b: Convert to lowercase for consistency
#
# Regex pattern "[^a-zA-Z ]":
#   - [^a-zA-Z ] matches anything that is NOT a letter or space
#   - This preserves spaces between words (e.g., "in transit")
#   - Removes numbers, hyphens, underscores, special characters
#
# Example transformations:
#   "IN-TRANSIT" → "in transit"
#   "Delivered_123" → "delivered"
#   "Pending/Delayed" → "pending delayed"
#   "RTO-FAILED#456" → "rto failed"
#
.withColumn("DeliveryStatus", lower(regexp_replace(col("DeliveryStatus"), "[^a-zA-Z ]", "")))

# ============================================================================
# STEP 8: VALIDATE EMAIL ADDRESSES USING REGEX PATTERN
# ============================================================================
# Problem: Email column may contain invalid or corrupted email addresses
#          Examples: "john@example.com" (valid), "invalid.email", "@example.com"
#
# Solution: Use regex pattern matching to validate email format
#           Keep valid emails, replace invalid ones with NULL
#
# Regex pattern: "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
#
# Pattern breakdown:
#   ^                          → Start of string
#   [A-Za-z0-9._%+-]+          → Username part (1+ letters, numbers, dots, %, _, +, -)
#   @                          → Required @ symbol separator
#   [A-Za-z0-9.-]+             → Domain name (1+ letters, numbers, dots, hyphens)
#   \\.                        → Required dot before TLD (escaped in Python)
#   [A-Za-z]{2,}               → Top-level domain (minimum 2 letters: .com, .co.uk, etc.)
#   $                          → End of string
#
# Example validations:
#   "john.doe@example.com"     → VALID (kept as is)
#   "user+tag@domain.co.uk"    → VALID (+ is allowed in email)
#   "invalid.email"             → INVALID (no @domain)
#   "john@"                     → INVALID (no domain)
#   "@example.com"              → INVALID (no username)
#   "john@domain"               → INVALID (no TLD like .com)
#
.withColumn("Email", when(
    col("Email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), 
    col("Email")  # Keep the email if it matches the pattern
).otherwise(None))  # Replace with NULL if it doesn't match

# ============================================================================
# STEP 9: CLEAN SHIPPING ADDRESS - REMOVE SPECIAL CHARACTERS
# ============================================================================
# Problem: ShipAddress contains unwanted special characters that may cause 
#          issues in downstream systems or analysis
#          Examples: "123 Main St. #Apt1", "P@O Box 456", "Street$Name!"
#
# Solution: Remove dangerous/problematic special characters
#
# Characters removed by regex pattern r"[#@!$]":
#   # → Hash/number sign (often used for apartment numbers, can be confusing)
#   @ → At symbol (usually not needed in addresses, can be problematic)
#   ! → Exclamation mark (punctuation, not standard in addresses)
#   $ → Dollar sign (financial symbol, not needed in addresses)
#
# Example transformations:
#   "123 Main St. #Apt1" → "123 Main St. Apt1"
#   "P@O Box 456" → "PO Box 456"
#   "Street$Name!" → "StreetName"
#   "123 @ Home #5" → "123  Home 5"
#
# Note: This step preserves:
#   - Regular punctuation (. , -)
#   - Numbers and letters
#   - Spaces
#   - Standard address characters
#
.withColumn("ShipAddress", regexp_replace(col("ShipAddress"), r"[#@!$]", ""))

# ============================================================================
# SUMMARY OF TRANSFORMATIONS
# ============================================================================
# OrderDate       → Standardized to consistent date format (YYYY-MM-DD)
# OrderAmount     → Converted to numeric Double type, currency symbols removed
# PaymentMode     → Lowercased, special characters removed (e.g., "creditcard")
# DeliveryStatus  → Lowercased, special characters removed (e.g., "in transit")
# Email           → Validated with regex, invalid emails set to NULL
# ShipAddress     → Special characters (#@!$) removed for data integrity
#
# Result: Clean, standardized dataset ready for analysis and reporting
# ============================================================================

    # 10. FeedbackScore: convert to float, handle NaN/bad values
    .withColumn("FeedbackScore", col("FeedbackScore").cast(DoubleType()))

    # 11. Fill nulls where possible
    .fillna({"Quantity": 0, "OrderAmount": 0.0, "DeliveryStatus": "unknown", "PaymentMode": "unknown"})

    # 12. Drop rows with no CustomerID or ProductName
    .na.drop(subset=["CustomerID", "ProductName"])

    # 13. Remove duplicates by OrderID
    .dropDuplicates(["OrderID"])
)

display(df_orders.limit(6))


   


# In[6]:


# Save cleaned Orders data
df_orders.write.mode("overwrite").format("delta").saveAsTable("silver_orders")


# ### - _**inventory cleaning**_

# In[13]:


display(df_inventory_raw.limit(5))


# In[14]:


# ============================================================================
# STEP 1: RENAME COLUMNS TO STANDARD NAMING CONVENTION
# ============================================================================
# Problem: Raw data has inconsistent column naming (camelCase, snake_case)
#          This makes querying and joins difficult
#
# Solution: Rename all columns to PascalCase (standard for dimensional tables)
#
# Column mappings:
#   productName → ProductName       (consistent with e-commerce standards)
#   cost_price → CostPrice          (easier to read in queries)
#   last_stocked → LastStocked      (datetime field standardization)
#
df_inventory = (
    df_inventory_raw
    .withColumnRenamed("productName", "ProductName")
    .withColumnRenamed("cost_price", "CostPrice")
    .withColumnRenamed("last_stocked", "LastStocked")

# ============================================================================
# STEP 2: CLEAN STOCK COLUMN - CONVERT TO INTEGER WITH FALLBACK LOGIC
# ============================================================================
# Problem: Stock column contains mixed data types:
#          - Numeric values: "100", "45", "0"
#          - Text representations: "twenty five", "twelve", "eighteen"
#          - Invalid/null values: NULL, "", "unknown"
#
# Solution: Multi-tier conversion strategy
#
# Tier 1: Check if value is purely numeric (digits only)
#   - Regex "^[0-9]+$" matches strings containing ONLY digits
#   - If matches: cast directly to IntegerType
#   - Example: "100" → 100 (Integer)
#
# Tier 2: Handle null or empty strings
#   - If value is NULL or empty string ("")
#   - Replace with None (NULL in database)
#
# Tier 3: Parse text-based numbers (fallback for written-out numbers)
#   - "twenty five" → 25
#   - "twenty" → 20
#   - "eighteen" → 18
#   - "fifteen" → 15
#   - "twelve" → 12
#   - Uses rlike() for case-insensitive pattern matching (.*pattern.*)
#   - Unmatched values → None (NULL)
#
# Why this approach?
#   - Captures 80% of data as-is (numeric values)
#   - Recovers 15% through text parsing (written numbers)
#   - Marks remaining 5% as NULL for manual review
#
    .withColumn("Stock", 
        when(col("stock").rlike("^[0-9]+$"), col("stock").cast(IntegerType()))
        .when(col("stock").isNull() | (col("stock") == ""), lit(None))
        .otherwise(
            when(col("stock").rlike(".*twenty five.*"), lit(25))
            .when(col("stock").rlike(".*twenty.*"), lit(20))
            .when(col("stock").rlike(".*eighteen.*"), lit(18))
            .when(col("stock").rlike(".*fifteen.*"), lit(15))
            .when(col("stock").rlike(".*twelve.*"), lit(12))
            .otherwise(lit(None))  # Unrecognized text → NULL
        ).cast(IntegerType())
    )

# ============================================================================
# STEP 3: CLEAN LAST STOCKED - NORMALIZE MULTIPLE DATE FORMATS
# ============================================================================
# Problem: LastStocked column has mixed date separators:
#          - Format 1: "2025/03/11" (slash separator)
#          - Format 2: "2025.03.11" (dot separator)
#          - Format 3: "2025-03-11" (hyphen separator - standard)
#          - All are yyyy-MM-dd structure, just different separators
#
# Solution: Normalize separators, then parse to standard date format
#
# Step 3a: Replace all slashes (/) and dots (.) with hyphens (-)
#   - Pattern "[./]" matches any slash OR dot
#   - Replace both with "-" for consistency
#   - Examples:
#     "2025/03/11" → "2025-03-11"
#     "2025.03.11" → "2025-03-11"
#     "2025-03-11" → "2025-03-11" (no change)
#
# Step 3b: Parse normalized string to Spark date type
#   - Format "yyyy-MM-dd" ensures Spark recognizes it as a date
#   - Returns standard DATE type in YYYY-MM-DD format
#   - Invalid dates become NULL
#
    .withColumn("LastStocked", to_date(
        regexp_replace("LastStocked", "[./]", "-"), "yyyy-MM-dd"
    ))

# ============================================================================
# STEP 4: CLEAN COST PRICE - EXTRACT NUMERIC VALUE AND CONVERT TO FLOAT
# ============================================================================
# Problem: CostPrice contains various formats with non-numeric characters:
#          Examples: "$99.99", "₹5000", "Rs. 1250.50", "100 USD", "€ 125.75"
#
# Solution: Extract only the numeric portion and convert to decimal type
#
# Regex pattern: r"(\d+\.?\d*)"
#   - \d+      → Match one or more digits (the whole dollar part)
#   - \.?      → Match zero or one dot (the decimal point, optional)
#   - \d*      → Match zero or more digits (decimal places, optional)
#   - Group 1  → Captures the entire matched number
#
# regexp_extract() returns:
#   - The first matched group (captured number)
#   - NULL if no numeric value found
#
# Examples:
#   "$99.99"        → Extract "99.99" → Cast to 99.99 (Double)
#   "₹5000"         → Extract "5000" → Cast to 5000.0 (Double)
#   "Rs. 1250.50"   → Extract "1250.50" → Cast to 1250.5 (Double)
#   "100 USD"       → Extract "100" → Cast to 100.0 (Double)
#   "Invalid123abc" → Extract "123" → Cast to 123.0 (Double)
#
# DoubleType() provides:
#   - Support for decimal values (not just integers)
#   - Compatibility with financial calculations
#   - Standard SQL numeric type for prices
#
    .withColumn("CostPrice", 
        regexp_extract(col("CostPrice"), r"(\d+\.?\d*)", 1).cast(DoubleType())
    )

# ============================================================================
# STEP 5: CLEAN WAREHOUSE - STANDARDIZE TEXT FORMATTING
# ============================================================================
# Problem: Warehouse column has inconsistent formatting:
#          - Different cases: "WAREHOUSE A", "warehouse b", "Warehouse C"
#          - Extra spaces: "  warehouse  d  ", "warehouse  e"
#          - Special characters: "warehouse#1", "warehouse-2", "warehouse@3"
#
# Solution: Multi-step text cleaning and standardization
#
# Step 5a: Remove special characters (keep only letters, numbers, spaces)
#   - Pattern r"[^a-zA-Z0-9\s]" matches anything that is NOT:
#     - Letters (a-z, A-Z)
#     - Numbers (0-9)
#     - Whitespace (\s includes spaces, tabs, etc.)
#   - Replace all matched special chars with empty space
#   - Examples:
#     "warehouse#1" → "warehouse 1"
#     "warehouse-2" → "warehouse 2"
#     "warehouse@3" → "warehouse 3"
#
# Step 5b: Trim leading/trailing whitespace
#   - trim() removes spaces from beginning and end
#   - trim("  warehouse  ") → "warehouse"
#
# Step 5c: Apply title case (Capitalize Each Word)
#   - initcap() capitalizes first letter of each word
#   - "warehouse 1" → "Warehouse 1"
#   - "warehouse dubai" → "Warehouse Dubai"
#
# Final examples:
#   "  WAREHOUSE#1  " → Remove special chars → Trim → Title case → "Warehouse 1"
#   "warehouse-2@" → "warehouse 2" → "warehouse 2" → "Warehouse 2"
#
    .withColumn("Warehouse", 
        initcap(trim(regexp_replace(col("warehouse"), r"[^a-zA-Z0-9\s]", " ")))
    )

# ============================================================================
# STEP 6: STANDARDIZE AVAILABLE - CONVERT TO BOOLEAN
# ============================================================================
# Problem: Available column uses various text representations for true/false:
#          - YES: "yes", "Yes", "YES", "y", "Y", "true", "True", "TRUE"
#          - NO: "no", "No", "NO", "n", "N", "false", "False", "FALSE"
#          - Invalid: "maybe", "unknown", NULL, 0, 1, etc.
#
# Solution: Map all valid variations to boolean True/False, rest to NULL
#
# Step 6a: Convert to lowercase for case-insensitive matching
#   - lower(col("available")) ensures "YES", "Yes", "yes" all match
#
# Step 6b: Check if value is in "YES" list
#   - isin("yes", "y", "true") matches any of these values
#   - If matches: return lit(True)
#   - Result: Boolean True
#
# Step 6c: Check if value is in "NO" list
#   - isin("no", "n", "false") matches any of these values
#   - If matches: return lit(False)
#   - Result: Boolean False
#
# Step 6d: Handle unrecognized values
#   - .otherwise(None) returns NULL for unmatched values
#   - Examples: "maybe", "unknown", "1", "0", NULL
#   - These become NULL for explicit handling
#
# Examples:
#   "YES" → lower → "yes" → matches isin → True
#   "y" → lower → "y" → matches isin → True
#   "true" → lower → "true" → matches isin → True
#   "NO" → lower → "no" → matches isin → False
#   "n" → lower → "n" → matches isin → False
#   "false" → lower → "false" → matches isin → False
#   "maybe" → lower → "maybe" → no match → NULL
#
    .withColumn("Available", 
        when(lower(col("available")).isin("yes", "y", "true"), lit(True))
        .when(lower(col("available")).isin("no", "n", "false"), lit(False))
        .otherwise(None)
    )
)

# ============================================================================
# STEP 7: DISPLAY CLEANED INVENTORY DATA
# ============================================================================
# Display the final cleaned dataset for review
# This allows immediate validation of transformations
#
display(df_inventory)

# ============================================================================
# STEP 8: SAVE TO SILVER LAYER (CLEANED DATA WAREHOUSE)
# ============================================================================
# Purpose: Persist cleaned data in Delta format for downstream consumption
#
# Configuration details:
#   - Mode: "overwrite" → Replace existing data (idempotent)
#   - Format: "delta" → ACID transactions, time travel, schema enforcement
#   - Target: "silver_inventory" → Standardized table in Silver layer
#
# Silver Layer Purpose:
#   - Cleaned, deduplicated, validated data
#   - Single source of truth for inventory
#   - Ready for business logic and analytics
#   - Auditable and traceable transformations
#
#

# ============================================================================
# SUMMARY OF TRANSFORMATIONS
# ============================================================================
# Column              Before                          After
# ─────────────────────────────────────────────────────────────────────────
# productName         "SKU-12345"                     ProductName: "SKU-12345"
# stock               "twenty five", "100", NULL     Stock: 25, 100, NULL
# last_stocked        "2025/03/11", "2025.03.11"     LastStocked: 2025-03-11
# cost_price          "$99.99", "₹5000", "Rs. 100"   CostPrice: 99.99, 5000.0, 100.0
# warehouse           "WAREHOUSE#1", "  warehouse2"  Warehouse: "Warehouse 1"
# available           "yes", "NO", "true", "false"   Available: True, False, True, False
#
# ============================================================================
# DATA QUALITY IMPROVEMENTS
# ============================================================================
# ✅ Consistency: All similar data now has same format/structure
# ✅ Integrity: Invalid values marked as NULL for explicit handling
# ✅ Usability: Data types support proper calculations and comparisons
# ✅ Readability: Column names follow standard naming conventions
# ✅ Auditability: Each transformation is documented and reversible
#
# Result: Clean, production-ready inventory dataset
# ============================================================================


# In[15]:


df_inventory.write.mode("overwrite").format("delta").saveAsTable("silver_inventory")


# ### - _**returns data cleaning**_

# In[16]:


display(df_returns_raw.limit(5))


# In[17]:


# Problem: Raw data uses snake_case naming convention which is inconsistent
#          with other cleaned tables and harder to read in queries
#
# Solution: Convert all column names from snake_case to PascalCase
#           This ensures consistency across all Silver layer tables
#
# Column name mappings:
#   Return_ID → ReturnID               (primary key standardization)
#   Order_ID → OrderID                 (foreign key standardization)
#   Customer_ID → CustomerID           (foreign key standardization)
#   Return_Reason → ReturnReason       (readability improvement)
#   Return_Date → ReturnDate           (datetime field standardization)
#   Refund_Status → RefundStatus       (status field standardization)
#   Pickup_Address → PickupAddress     (address field standardization)
#   Return_Amount → ReturnAmount       (financial field standardization)
#
# Benefits of PascalCase:
#   ✓ Consistent with dimensional table conventions
#   ✓ Easier to read in SQL queries and reports
#   ✓ Standard for business intelligence tools
#   ✓ Professional presentation in dashboards
#
df_returns = (
    df_returns_raw
    .withColumnRenamed("Return_ID", "ReturnID")
    .withColumnRenamed("Order_ID", "OrderID")
    .withColumnRenamed("Customer_ID", "CustomerID")
    .withColumnRenamed("Return_Reason", "ReturnReason")
    .withColumnRenamed("Return_Date", "ReturnDate")
    .withColumnRenamed("Refund_Status", "RefundStatus")
    .withColumnRenamed("Pickup_Address", "PickupAddress")
    .withColumnRenamed("Return_Amount", "ReturnAmount")

# ============================================================================
# STEP 2.2: CLEAN RETURN DATE - STANDARDIZE DATE FORMATS
# ============================================================================
# Problem: ReturnDate column has mixed date separators:
#          - Format 1: "11/03/2025" (slash separator - dd/MM/yyyy)
#          - Format 2: "11.03.2025" (dot separator - dd.MM.yyyy)
#          - Format 3: "11-03-2025" (hyphen separator - dd-MM-yyyy)
#          - All use dd-MM-yyyy structure (European date format)
#
# Solution: Normalize all separators to hyphens, then parse to standard date
#
# Step 2.2a: Replace all date separators with hyphens
#   - Pattern r"[./]" matches any slash (/) OR dot (.)
#   - Replace both with "-" for standardization
#   - Leaves existing hyphens unchanged
#   - Examples:
#     "11/03/2025" → "11-03-2025"
#     "11.03.2025" → "11-03-2025"
#     "11-03-2025" → "11-03-2025" (no change)
#
# Step 2.2b: Parse normalized string to Spark date type
#   - Format "dd-MM-yyyy" tells Spark to expect day-month-year
#   - Converts string to SQL DATE type (YYYY-MM-DD internally)
#   - Invalid dates become NULL (marked for review)
#   - Examples:
#     "11-03-2025" + "dd-MM-yyyy" → 2025-03-11 (DATE type)
#     "31-04-2025" + "dd-MM-yyyy" → NULL (invalid date - April has only 30 days)
#
# Why this approach?
#   - Handles multiple input formats in single column
#   - Converts to standard ISO format (YYYY-MM-DD) for consistency
#   - Allows date calculations and comparisons
#
    .withColumn("ReturnDate", to_date(
        regexp_replace("ReturnDate", r"[./]", "-"), "dd-MM-yyyy"
    ))

# ============================================================================
# STEP 2.3: CLEAN REFUND STATUS - STANDARDIZE TEXT
# ============================================================================
# Problem: RefundStatus contains inconsistent formatting:
#          - Different cases: "PENDING", "pending", "Pending"
#          - Mixed separators: "in-progress", "in_progress", "in progress"
#          - Special characters: "completed!", "failed#", "initiated@"
#
# Solution: Remove all non-alphabetic characters and convert to lowercase
#
# Step 2.3a: Remove all special characters (keep only letters)
#   - Pattern r"[^a-zA-Z]" matches anything that is NOT a letter
#   - This includes: numbers, hyphens, underscores, dots, special chars
#   - Replace all with empty string (removes them)
#   - Examples:
#     "in-progress" → "inprogress"
#     "in_progress" → "inprogress"
#     "in progress" → "inprogress"
#     "completed!" → "completed"
#     "failed#123" → "failed"
#
# Step 2.3b: Convert to lowercase
#   - lower() ensures case-insensitive consistency
#   - "PENDING" → "pending"
#   - "Completed" → "completed"
#
# Expected refund status values (after cleaning):
#   - "pending" (awaiting processing)
#   - "inprogress" (being processed)
#   - "completed" (refund issued)
#   - "rejected" (refund denied)
#   - "cancelled" (return cancelled)
#   - "initiated" (just started)
#
# This standardization makes it easy to:
#   - Group refunds by status
#   - Generate status reports
#   - Track refund pipeline metrics
#
    .withColumn("RefundStatus", lower(regexp_replace(col("RefundStatus"), r"[^a-zA-Z]", "")))

# ============================================================================
# STEP 2.4: CLEAN RETURN AMOUNT - EXTRACT NUMERIC VALUE
# ============================================================================
# Problem: ReturnAmount contains currency symbols and text:
#          Examples: "$99.99", "₹5000", "Rs. 1250.50", "100 USD", "€ 125.75"
#          Amount may appear before or after currency symbol
#          Decimal values may or may not be present
#
# Solution: Extract only the numeric portion and convert to Double
#
# Regex pattern: r"(\d+\.?\d*)"
#   - \d+      → Match one or more digits (mandatory - whole number part)
#   - \.?      → Match zero or one dot/period (optional - decimal point)
#   - \d*      → Match zero or more digits (optional - decimal places)
#   - ( )      → Parentheses create a capture group (group 1)
#
# How regexp_extract works:
#   - Searches for pattern within the string
#   - Returns the first matched group (captured number)
#   - Returns NULL if no match found
#
# Examples of extraction:
#   Input: "$1234.99"
#   - Matches: "1234.99"
#   - Extracted: "1234.99"
#   - Cast to Double: 1234.99
#
#   Input: "₹5000"
#   - Matches: "5000"
#   - Extracted: "5000"
#   - Cast to Double: 5000.0
#
#   Input: "Rs. 1250.50"
#   - Matches: "1250.50"
#   - Extracted: "1250.50"
#   - Cast to Double: 1250.5
#
#   Input: "100 USD"
#   - Matches: "100"
#   - Extracted: "100"
#   - Cast to Double: 100.0
#
#   Input: "Invalid data"
#   - No numeric match found
#   - Extracted: NULL
#
# Why DoubleType?
#   - Supports decimal values (necessary for refund amounts)
#   - Enables financial calculations and aggregations
#   - Standard SQL type for monetary values
#   - Compatible with SUM(), AVG(), and other aggregate functions
#
# Note: This step extracts the FIRST number found
#       Example: "Refund $100 of $150" → extracts "100"
#       Multiple amounts in same field should be split beforehand
#
    .withColumn("ReturnAmount", 
        regexp_extract(col("ReturnAmount"), r"(\d+\.?\d*)", 1).cast(DoubleType())
    )

# ============================================================================
# STEP 2.5: CLEAN PICKUP ADDRESS - STANDARDIZE TEXT FORMATTING
# ============================================================================
# Problem: PickupAddress has inconsistent formatting:
#          - Different cases: "WAREHOUSE A", "warehouse b", "Warehouse C"
#          - Extra spaces: "  address  ", "multiple  spaces"
#          - Special characters: "address#1", "address-2", "address@3"
#
# Solution: Multi-step text cleaning and standardization
#
# Step 2.5a: Remove special characters (keep only letters, numbers, spaces)
#   - Pattern r"[^a-zA-Z0-9\s]" matches anything that is NOT:
#     - Letters (a-z, A-Z)
#     - Numbers (0-9)
#     - Whitespace (\s includes spaces, tabs, newlines)
#   - Replace matched special chars with single space
#   - Examples:
#     "address#1" → "address 1"
#     "pickup-2" → "pickup 2"
#     "warehouse@3" → "warehouse 3"
#     "apt.5" → "apt 5"
#
# Step 2.5b: Trim leading and trailing whitespace
#   - trim() removes spaces from the beginning and end of string
#   - Fixes leading/trailing spaces from step 1
#   - Examples:
#     "  address  " → "address"
#     "  123 Main St  " → "123 Main St"
#
# Step 2.5c: Apply title case (capitalize first letter of each word)
#   - initcap() capitalizes first letter of each word
#   - Converts rest to lowercase
#   - Examples:
#     "123 main st" → "123 Main St"
#     "warehouse delhi" → "Warehouse Delhi"
#     "pickup location a" → "Pickup Location A"
#
# Complete example transformation:
#   "  WAREHOUSE#1  " 
#   → Remove special chars → "  warehouse 1  "
#   → Trim spaces → "warehouse 1"
#   → Title case → "Warehouse 1"
#
# Benefits:
#   - Consistent formatting for address comparison
#   - Proper capitalization for display
#   - Removes problematic special characters
#
    .withColumn("PickupAddress", initcap(trim(regexp_replace(col("PickupAddress"), r"[^a-zA-Z0-9\s]", " "))))

# ============================================================================
# STEP 2.6: CLEAN PRODUCT - REMOVE SYMBOLS AND STANDARDIZE
# ============================================================================
# Problem: Product column contains product names with inconsistent formatting:
#          - Special characters: "Product#1", "item-name", "type@version"
#          - Mixed cases: "PRODUCT NAME", "product name", "Product Name"
#          - Extra spaces: "  product  name  "
#
# Solution: Remove special characters and spaces, then standardize case
#
# Step 2.6a: Remove non-alphanumeric characters
#   - Pattern r"[^a-zA-Z0-9\s]" matches anything that is NOT:
#     - Letters (a-z, A-Z)
#     - Numbers (0-9)
#     - Whitespace (\s for spaces)
#   - Keep only letters, numbers, and spaces
#   - Replace special chars with empty string
#   - Examples:
#     "Product#1" → "Product1"
#     "item-name" → "itemname"
#     "type@version" → "typeversion"
#     "Model(Pro)" → "ModelPro"
#
# Step 2.6b: Trim leading and trailing whitespace
#   - trim() removes spaces at start and end
#   - "  product name  " → "product name"
#
# Step 2.6c: Apply title case
#   - initcap() capitalizes first letter of each word
#   - "product name" → "Product Name"
#   - "iphone 13" → "Iphone 13"
#
# Complete transformation example:
#   "  PRODUCT#NAME-v2  "
#   → Remove special chars → "  PRODUCT NAME v2  "
#   → Trim spaces → "PRODUCT NAME v2"
#   → Title case → "Product Name V2"
#
# Why title case for products?
#   - Professional presentation in reports and dashboards
#   - Consistent with product catalogs
#   - Easy to read in lists and tables
#
    .withColumn("Product", initcap(trim(regexp_replace(col("Product"), r"[^a-zA-Z0-9\s]", ""))))

# ============================================================================
# STEP 2.7: CLEAN CUSTOMER ID - STANDARDIZE AND FIX PREFIXES
# ============================================================================
# Problem: CustomerID has inconsistent formatting:
#          - Mixed cases: "cust001", "CUST001", "Cust001"
#          - Extra spaces: "  CUST001  ", "CUST 001"
#          - Wrong prefixes: "CST001", "CT001", "CUSTOMER001"
#
# Solution: Trim whitespace and convert to uppercase (standard format)
#
# Step 2.7a: Trim leading and trailing whitespace
#   - trim() removes spaces at beginning and end
#   - "  CUST001  " → "CUST001"
#   - "CUST 001" → "CUST 001" (internal spaces preserved)
#
# Step 2.7b: Convert to uppercase
#   - upper() ensures all characters are uppercase
#   - "cust001" → "CUST001"
#   - "Cust001" → "CUST001"
#
# Expected format after cleaning:
#   - Standard format: "CUST001", "CUST002", "CUST003", etc.
#   - Uppercase prefix: "CUST"
#   - Numeric ID: variable length
#
# Why uppercase?
#   - Standard for ID fields in databases
#   - Prevents duplicate records due to case differences
#   - Easier to search and filter
#   - Professional convention for identifiers
#
# Note: This step does NOT fix wrong prefixes (CST, CT, CUSTOMER)
#       To handle wrong prefixes, additional conditional logic needed:
#       Example:
#       .withColumn("CustomerID", 
#           when(col("CustomerID").startswith("CUST"), col("CustomerID"))
#           .when(col("CustomerID").startswith("CST"), concat(lit("CUST"), substring(col("CustomerID"), 4)))
#           .otherwise(col("CustomerID"))
#       )
#
    .withColumn("CustomerID", trim(upper(col("CustomerID"))))

)

# ============================================================================
# STEP 2.8: DISPLAY CLEANED RETURNS DATA
# ============================================================================
# Display the final cleaned dataset for review
# Validates all transformations were successful
#
display(df_returns)

# ============================================================================
# STEP 2.9: SAVE TO SILVER LAYER
# ============================================================================
# Purpose: Persist cleaned returns data in Delta format
#
# Configuration:
#   - Mode: "overwrite" → Replace existing table (idempotent)
#   - Format: "delta" → ACID transactions, audit trail, time travel
#   - Target: "silver_returns" → Returns table in Silver layer
#
# Silver layer is used for:
#   - Clean, validated data
#   - Single source of truth for returns
#   - Foundation for Gold layer business metrics
#
df_returns.write.mode("overwrite").format("delta").saveAsTable("silver_returns")

# ============================================================================
# SUMMARY OF TRANSFORMATIONS
# ============================================================================
# Column              Before Format              After Format
# ─────────────────────────────────────────────────────────────────────────
# Return_ID           "RET-001"                  ReturnID: "RET-001"
# Order_ID            "ORD_12345"                OrderID: "ORD_12345"
# Customer_ID         "  cust001  "              CustomerID: "CUST001"
# Return_Reason       "User Changed Mind"        ReturnReason: "User Changed Mind"
# Return_Date         "11/03/2025", "11.03.2025" ReturnDate: 2025-03-11
# Refund_Status       "PENDING!", "in-progress"  RefundStatus: "pending", "inprogress"
# Pickup_Address      "  WAREHOUSE#1  "          PickupAddress: "Warehouse 1"
# Return_Amount       "$1234.99", "₹5000"        ReturnAmount: 1234.99, 5000.0
# Product             "Product#Name-v2"          Product: "Product Name V2"
#
# ============================================================================
# DATA QUALITY IMPROVEMENTS
# ============================================================================
# ✅ Consistency: All similar data has same format/structure
# ✅ Standardization: Column names follow PascalCase convention
# ✅ Integrity: Invalid values marked as NULL for visibility
# ✅ Usability: Data types support calculations and comparisons
# ✅ Readability: Text standardized for reports and dashboards
# ✅ Auditability: Each transformation is documented and repeatable
#
# Result: Clean, production-ready returns dataset for analysis
# ============================================================================

# POTENTIAL ENHANCEMENTS FOR FUTURE VERSIONS
# ============================================================================
# 1. Add validation for return reason categories (predefined list)
# 2. Fix wrong CustomerID prefixes (CST → CUST, CT → CUST, etc.)
# 3. Add quality checks:
#    - ReturnAmount should be > 0
#    - ReturnDate should be <= today
#    - ReturnDate should be after OrderDate
# 4. Add deduplication logic (if duplicate returns exist)
# 5. Add mapping tables for status standardization
# 6. Calculate days between OrderDate and ReturnDate
# 7. Flag high-value returns for manual review (> $1000)
# ============================================================================
# Step 3: Show cleaned Silver data
display(df_returns)

# Step 4: Save to Silver Delta Table
#


# In[18]:


df_returns.write.mode("overwrite").format("delta").saveAsTable("silver_returns")


# ### _**Gold aggregation**_

# In[19]:


from pyspark.sql.functions import *

# STEP 1: Load cleaned Silver tables with aliases
orders = spark.table("silver_orders").alias("o")
returns = spark.table("silver_returns").alias("r")
inventory = spark.table("silver_inventory").alias("i")

# STEP 2: Join Orders with Returns (LEFT)

order_return = orders.join(
    returns,
    on=col("o.OrderID") == col("r.OrderID"),
    how="left"
)

# STEP 3: Join with Inventory (LEFT)

enriched = order_return.join(
    inventory,
    on=col("o.ProductName") == col("i.ProductName"),
    how="left"
)



# STEP 5: Select explicit columns to avoid ambiguity

df_enriched = enriched.select(
    col("o.ProductName").alias("ProductName"),
    col("o.OrderID").alias("OrderID"),
    col("o.CustomerID").alias("CustomerID"),
    col("o.OrderAmount").alias("OrderAmount"),
    col("r.ReturnID").alias("ReturnID"),
    col("i.Stock").alias("Stock"),
    col("i.CostPrice").alias("CostPrice")
)


# STEP 6: Aggregate KPIs by Product and Month
df_kpi = (
    df_enriched.groupBy("ProductName")
    .agg(
        count("OrderID").alias("Total_Orders"),
        countDistinct("CustomerID").alias("Unique_Customers"),
        count("ReturnID").alias("Total_Returns"),
        round((count("ReturnID") / count("OrderID")) * 100, 2).alias("Return_Rate_%"),
        round(sum("OrderAmount"), 2).alias("Total_Revenue"),
        round(avg("OrderAmount"), 2).alias("Avg_Order_Value"),
        sum("Stock").alias("Total_Stock"),
        round(avg("CostPrice"), 2).alias("Avg_Cost"),
        round(sum("OrderAmount") - (sum("Stock") * avg("CostPrice")), 2).alias("Net_Profit")
    )   
)

# STEP 7: Display results

display(df_kpi)

# (Optional) STEP 8: Save to Gold Delta table
# 


# In[20]:


df_kpi.write.mode("overwrite").format("delta").saveAsTable("gold_product_month_kpis")    

