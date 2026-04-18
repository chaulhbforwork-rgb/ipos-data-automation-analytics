iPOS Sales Automation & Financial Data Pipeline
🎯 Project Overview
This project automates the end-to-end process of extracting daily sales data from the iPOS (Fabi) POS system, performing financial data transformation, and preparing it for high-level business intelligence reporting.

It eliminates 5+ hours of manual work per week, ensuring 100% data integrity for financial reconciliation and operational analysis.

🛠 Tech Stack
Language: Python 3.x

Automation: Selenium WebDriver (Automated Token & Header Retrieval)

Data Processing: Pandas, NumPy (Vectorized operations for performance)

APIs: Requests (Direct communication with iPOS backend)

Deployment: Windows Batch Scripting (.bat) for one-click execution

🚀 Key Features & Logic
Automated Authentication: Utilizes Selenium to capture network performance logs, automatically retrieving dynamic Auth Tokens without manual intervention.

Complex Data Transformation: Uses ast.literal_eval to parse nested string-based transaction data into structured formats.

Financial Correction Logic: - Implemented a specialized dictionary-based matching system to correct Unit Prices for specific toppings.

Recalculates total Order Values to ensure financial reports match actual bank settlements.

Dynamic File Management: Automatically organizes and exports processed data into timestamped Excel files for audit trails.

📁 Project Structure
scripts/: Contains the core Python engine (Scraping_data_ban_Pos.py) and the .bat automation file.

data/raw/: Samples of raw JSON/CSV data fetched directly from the API.

data/processed/: Final, cleaned, and CFA-logic-aligned Excel files.

presentation/: A comprehensive PDF case study (created via Canva) detailing the ETL workflow, business logic, and Looker Studio visualization.

📊 Business Impact
Efficiency: Transitioned from manual Excel downloads to a 100% automated "click-and-run" system.

Accuracy: Eliminated human error in topping price adjustments and revenue calculations.

Scalability: The pipeline is designed to handle multi-branch data extraction with minimal configuration changes.

Developed by Le Hoang Bao Chau - Operational Excellence | Finance Professional | CFA Level I Candidate.
