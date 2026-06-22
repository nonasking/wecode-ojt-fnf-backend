# wecode-ojt-fnf-backend

## About the F&F industry-collaboration project — Back-end

- A clone of F&F's internal company dashboard.
- F&F's internal dashboard processes and visualizes sales data to deliver meaningful business insights to in-house users.
- We implemented the Category Sales Analysis page and the Style Ranking page of that dashboard.
- We worked with real data pulled from F&F's internal database. (For security reasons, the demo video uses mock data created by the team.)

### Team & timeline

- Period: 2020/1/24 ~ 2020/2/23
- Team:
  - Back-end (3): Jaeyeop Kim, Minsung Kang, Wonseok Ji
  - Front-end (4): [Frontend GitHub repository](https://github.com/KimJeongHyun/wecode-ojt-fnf-frontend)

### Background

- This project was carried out by 7 wecode 28th-cohort students as part of an industry collaboration between wecode and F&F.

<br>

## Collaboration tools
- Slack, GitHub

## Tech stack

### Shared
- RESTful API

### Back-end
- Python / Django
- Pandas
- PostgreSQL
- AWS Redshift

## Features

### Shared

#### Filter
- Select a brand's data categories in the filter to view the data you want.

### Category Sales Analysis page

#### Search-volume / keyword data visualization
- View prior-year search volume / current-year search volume / year-over-year data in a table, by brand.
- View search-volume trends for general and own-brand keywords as charts.
- View search-volume trends for competitor keywords as charts.

#### (Category sales analysis / Wonseok Ji's part)

#### Weekly sales performance analysis
- Process per-subcategory sales data to fit charts and tables.
- Process per-domain (e.g. pattern) sales data to fit charts and tables.
- Process per-item sales volume, stock quantity, etc. to fit tables.
- Process per-country sales data to fit charts and tables.
- Process per-channel sales data to fit charts and tables.

### Style Ranking page

#### Ranking items & summary
- Compute statistics for the Top 5, 20, 50, 100, and Total rankings.
- Process Top 200 items and their sales-volume statistics.
- Compute sales-related statistics matching the given conditions.

#### Detailed visualization of a specific product
- View the data you want for a specific product via radio-button selection.
- View a specific product's revenue trend in a table and chart.
- View weekly sales data in a table.
- View per-channel / per-store sales data in a table.

<br>

## Reference

- This project was built for learning purposes, referencing F&F's internal dashboard.
- Although it is a production-grade project, it was made for study; using this code for profit or redistributing it without permission may cause legal issues.
