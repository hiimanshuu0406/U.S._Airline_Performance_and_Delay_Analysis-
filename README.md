# U.S. Airline Performance & Delay Analysis ✈️
**📌 Summary**

This project analyzes over 5 million U.S. domestic flight records to uncover performance patterns, identify delay causes, and evaluate airline/airport efficiency. The end-to-end workflow involved data cleaning, transformation, SQL analysis, and interactive visualization using Tableau.

**📊 Dasboard Prewview**

Dashboard 1
<img width="900" height="500" alt="Image" src="https://github.com/user-attachments/assets/893d1c7d-49c3-4e12-9fc8-4d9ef27419fd" />

Dashboard 2
<img width="900" height="500" alt="Image" src="https://github.com/user-attachments/assets/58cdb0f7-11ce-46f7-956f-62e2e336296e" />

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

**📂 Dataset Access**

Due to large file size, the raw datasets are hosted on Google Drive:

👉 https://drive.google.com/file/d/1RlO6jO-Bva7Um6tvItOe0an8KSJmSyAG/view?usp=sharing

👉 https://drive.google.com/file/d/1OC0BlKzgndoHruKnjk3_oWwLy4bGy55_/view?usp=sharing

👉 https://drive.google.com/file/d/1QPa_qILmbEF5IguvGjV5dK1w5I7RPjh3/view?usp=sharing
