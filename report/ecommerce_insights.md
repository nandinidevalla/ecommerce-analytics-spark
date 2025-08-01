# 📊 Business Insights: E-Commerce Analytics with Spark

This document summarizes insights derived from analyzing 100K+ customer transactions using Apache Spark (SQL + DataFrame API). The dataset originates from a Brazilian marketplace and covers orders, payments, products, reviews, and customer geography — allowing us to uncover actionable trends and make strategic recommendations across the e-commerce funnel.

---

## 💰 Revenue & Sales Trends

- **Total Revenue**: $16M+ across 2016–2018
- **Average Order Value**: $160.99
- **Peak Sales Months**:
  - 📈 November 2017: $1.17M
  - 📈 April 2018: $1.16M
- **Revenue Drop**: September 2018 fell to $166 — possibly a data or operational anomaly

**Recommendations**:
- Double down on marketing during Nov–Jan and April peaks  
- Investigate September drop for operational gaps  
- Boost AOV through upselling, product bundles, and threshold-based discounts

---

## 🛍️ Top Products & Payments

- **Top Categories**: Home & Living, Health & Beauty, Tech Accessories
- **Top Payment Methods**:
  - Credit Card (majority)
  - Boleto (bank slip) — popular offline option in Brazil

**Opportunities**:
- Target best-selling categories with seasonal promos  
- Offer credit card installment plans & boleto incentives  

---

## 👥 Customer Behavior & Retention

- **93%** of users are one-time buyers  
- **Only 11** customers had more than one order
- **Top Cities**: São Paulo, Rio de Janeiro (high demand zones)

**Strategies**:
- Loyalty programs to convert one-timers into repeat buyers  
- Personalized email offers post-purchase  
- Regional promotions in top-performing cities  

---

## 🚚 Shipping & Logistics Insights

- **Average Shipping Time**: 12.3 days
- **Slowest Regions**:  
  - Roraima (29.3 days)  
  - Amapá (27.1 days)  
  - Amazonas (26.3 days)
- **Worst Delivery Delay**: 188 days  
- **Top 10 delayed orders** all exceeded 159 days

**Fixes**:
- Audit slowest carriers and underperforming lanes  
- Establish regional fulfillment in North Brazil  
- Add express shipping options for time-sensitive orders  

---

## 🌟 Customer Reviews & Sentiment

- **Review Score = 5** accounted for **57%** of all ratings  
- **83%** of all reviews were high (4–5)  
- **Most common complaint words** in low reviews: “delay,” “wrong,” “missing”

**Insight**: Delivery delays are a major source of negative reviews

**Action**:
- Reward on-time deliveries with loyalty points  
- Improve real-time tracking and resolution SLAs  
- Monitor review text for early warning signals  

---

## 📅 Seasonality & Time Trends

- **Highest Sales Month**: April 2018 ($1.16M)  
- **Best Seasons**: Spring & Summer  
- **Lowest Sales**: Fall months

**Opportunity**:
- Launch spring campaigns and summer essentials bundles  
- Use Fall for backend inventory and ops improvement  

---

## 🔍 Fraud Signals

- Detected **outlier transactions** > $10,000  
- Some show mismatched delivery & review patterns

**Suggestions**:
- Flag high-value orders for manual review  
- Cross-analyze customer behavior and region  
- Study high-value products for special fulfillment handling  

---

## 🎯 Final Recommendations

### For Retention:
- Implement loyalty tiers and referral bonuses  
- Personalize offers using review + order history  
- Improve post-purchase engagement (surveys, follow-ups)

### For Logistics:
- Expand regional partnerships  
- Penalize SLA breaches; incentivize fast carriers  
- Track shipping at batch-level for better ETA accuracy

### For Business:
- Optimize pricing + marketing around seasonal spikes  
- Use Spark jobs for real-time fraud detection in pipeline  
- Invest in better review prediction and sentiment tagging

---

## 👩‍💻 Author

**Nandini Priya Devalla**  
Graduate Student – Business Analytics, Purdue University  
[LinkedIn](https://www.linkedin.com/in/nandini-devalla)
