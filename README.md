# U.S. Airline Performance & Delay Analysis ✈️
**📌 Summary**

This project analyzes over 5 million U.S. domestic flight records to uncover performance patterns, identify delay causes, and evaluate airline/airport efficiency. The end-to-end workflow involved data cleaning, transformation, SQL analysis, and interactive visualization using Tableau.

**❓ Business Problem**
1. Flight delays are costly to both airlines and passengers. This project aims to answer:
2. Which airlines and airports have the most/least delays?
3. What are the primary reasons for delays across months and airlines?
4. How do seasonal trends impact flight volumes and delays?

**🔍 Methodology**
1. Cleaned and structured raw flight, airline, and airport datasets (5M+ rows) in PostgreSQL.
2. Created new columns (e.g., formatted times, cancellation reasons) and replaced invalid FAA codes with "Unknown."
3. Joined tables, ran SQL queries, and built Tableau dashboards for insights.

**🛠️ Skills & Tools**
1. SQL (PostgreSQL): Data cleaning, joins, queries, views
2. Tableau: Interactive dashboards and visualizations
3. Excel: Initial exploration & dataset checks

**📊 Results & Business Recommendations**
1. Southwest Airlines operated the highest number of flights, while Virgin America had the least.
2. Aircraft delays caused the most lost minutes; security delays were minimal.
3. Spirit and Frontier Airlines had the poorest delay performance; Hawaiian and Alaska Airlines performed best.
4. Airlines should prioritize aircraft maintenance scheduling to reduce aircraft delays.
5. Airports should allocate additional resources during peak months (March & June) to handle higher traffic.
6. Low-performing airlines can adopt best practices from consistently reliable carriers (e.g., Hawaiian).
