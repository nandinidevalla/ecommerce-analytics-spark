# 🛍️ E-Commerce Analytics with Apache Spark

This project explores customer behavior, sales trends, shipping performance, review sentiment, and fraud detection using the **Brazilian E-Commerce Public Dataset by Olist**. Built with **Apache Spark** (SQL + DataFrame API) on **Databricks**, the project simulates end-to-end analytics workflows used by modern retail and logistics companies.

---

## 📦 Dataset Overview

- **Source**: [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Volume**: 100,000+ orders across multiple relational tables
- **Tables Included**: Orders, Products, Customers, Payments, Reviews, Sellers, Geolocation
- **Use Cases**: Churn analysis, delivery optimization, review analysis, fraud detection

---

## 🧰 Tech Stack

| Category      | Tools / Frameworks                          |
|---------------|---------------------------------------------|
| 💻 Platform   | Databricks (Community Edition)              |
| ⚙️ Engine     | Apache Spark                                |
| 📊 APIs       | Spark SQL, Spark DataFrame API              |
| 🐍 Language   | Python                                      |
| 🗂️ Storage    | Parquet format                              |
| 📈 Methods    | Aggregation, Joins, Filtering, GroupBy, UDFs |

---

## 📊 Key Business Questions Addressed

- What drives customer retention and loyalty?
- Which regions suffer from poor delivery SLAs?
- How do reviews correlate with delivery speed?
- What are the top-performing product categories?
- Where do fraud-like transactions occur?

---

## 🧠 Insights Uncovered

- 93% of users are one-time buyers → opportunity for loyalty marketing
- Delays over 150+ days detected in northern regions → logistics gap
- 83% of reviews are 4–5 stars, but low reviews center on "delay" or "wrong item"
- Sales peak in Nov (Black Friday) and spring months → seasonal targeting opportunity
- Outlier transactions > $10,000 show review mismatches → fraud alerts

📄 Full summary: [`report/ecommerce_insights.md`](report/ecommerce_insights.md)

---

## 🗂️ Folder Structure

```
notebooks/
├── ecommerce_analysis_dataframe_api.ipynb # Spark DataFrame API implementation
├── ecommerce_analysis_spark_sql.sql # Spark SQL queries

report/
└── ecommerce_insights.md # Business insights and strategic recommendations

README.md # You're here!

```

---

## ✅ Learning Outcomes

- Practiced scalable joins across multi-file relational data
- Performed deep-dive analytics with Spark DataFrame and SQL APIs
- Designed a realistic, production-like workflow using Databricks
- Generated actionable recommendations across sales, logistics, and customer behavior

---

## 👩‍💻 Author

**Nandini Priya Devalla**  
Graduate Student – Business Analytics, Purdue University  
[LinkedIn](https://www.linkedin.com/in/nandini-devalla)
