# ✈️ Flight Operations Data Warehouse & BI Solution
 
<div align="center">
 
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL%20Server%202022-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)
![SSIS](https://img.shields.io/badge/SSIS-ETL%20Pipeline-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
![SSAS](https://img.shields.io/badge/SSAS-OLAP%20Cube-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Excel](https://img.shields.io/badge/Excel-OLAP%20Analysis-217346?style=for-the-badge&logo=microsoftexcel&logoColor=white)
 
**End-to-end enterprise data warehouse built on 7 million US domestic flight records**  
*Raw BTS CSV → Normalised OLTP → Star Schema DW → SSAS Cube → Power BI Dashboards*
 
[View Power BI Reports](#-power-bi-reports) · [Data Architecture](#-architecture) · [ETL Pipeline](#-etl-pipeline) · [OLAP Cube](#-ssas-cube--olap-operations) · [Getting Started](#-getting-started)
 
</div>
 
---

## 📌 Project Overview
 
This project implements a **production-grade data warehouse and business intelligence solution** for US domestic aviation analytics. Starting from a single 35-column, 7-million-row flat CSV published by the Bureau of Transportation Statistics and the dataset is being sourced from Kaggle 
(https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024). The project delivers a fully operational analytical stack: normalised source files, a SQL Server star schema, a multi-source SSIS ETL pipeline, an SSAS multidimensional cube, Excel OLAP analysis, and four published Power BI reports.
 
The work spans the complete data engineering and BI lifecycle — from raw data profiling and source preparation, through dimensional modelling and ETL development, to OLAP cube construction and executive-level reporting.

### Business Questions Answered
- Which US carriers have the worst on-time performance and why?
- Which delay type (carrier, weather, NAS, security, late aircraft) dominates by route and season?
- How does cancellation rate vary by region, carrier, and time of year?
- Which routes and airports are the highest-risk for delay propagation?
- How long does the post-flight operational processing pipeline take by flight type?
 
---
 
## 🏗 Architecture
 
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOURCE LAYER                                       │
│                                                                               │
│  ┌──────────────────┐  ┌─────────────────────┐  ┌────────────────────────┐  │
│  │  CSV Flat Files  │  │  SQL Server DB       │  │  Excel Workbook        │  │
│  │                  │  │  (AirlineOLTP)       │  │  (reference_data.xlsx) │  │
│  │ flight_ops.csv   │  │                      │  │                        │  │
│  │ 7,079,081 rows   │  │  Airline  (15 rows)  │  │  CancellationReasons   │  │
│  │                  │  │  Airport  (348 rows) │  │  RegionMapping         │  │
│  │ delay_records.csv│  │  Flight (600K rows)  │  │  (US Census Regions)   │  │
│  └────────┬─────────┘  └──────────┬──────────┘  └───────────┬────────────┘  │
└───────────┼─────────────────────  ┼  ──────────────────────  ┼  ────────────┘
            │                       │                           │
┌───────────▼───────────────────────▼───────────────────────────▼─────────────┐
│                        SSIS ETL LAYER  (3 packages)                           │
│                                                                               │
│  Package1_LoadDims  →  Package2_LoadFact  →  Package3_AccumUpdate            │
│  (SCD Type 2, Lookups,   (7M rows, 6 FK       (Incremental UPDATE,           │
│   Region enrichment)      Lookups, Merge Join)  process hours KPI)           │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────────────┐
│                     DATA WAREHOUSE  (FlightDW — Star Schema)                  │
│                                                                               │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  ┌────────────────┐  │
│  │  Dim_Date   │  │  Dim_Airline │  │   Dim_Airport  │  │   Dim_Route    │  │
│  │  366 rows   │  │  SCD Type 2  │  │ Role-playing   │  │  600K rows     │  │
│  │  Hierarchy  │  │  60 rows     │  │  348 airports  │  │  Dist. bands   │  │
│  └──────┬──────┘  └──────┬───────┘  └───────┬────────┘  └───────┬────────┘  │
│         │                │                   │                    │           │
│         └────────────────┴─────────┬─────────┴────────────────────┘           │
│                                    │                                           │
│                      ┌─────────────▼──────────────┐                           │
│                      │   Fact_FlightOperation      │                           │
│                      │   7,079,020 rows            │                           │
│                      │   15 additive measures      │                           │
│                      │   3 accumulating columns    │                           │
│                      └────────────────────────────┘                           │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
              ┌─────────────────────┴──────────────────────┐
              │                                             │
┌─────────────▼──────────────┐          ┌──────────────────▼──────────────────┐
│     SSAS OLAP CUBE          │          │         POWER BI SERVICE             │
│     FlightDW_SSAS           │          │                                      │
│                             │          │  Report 1: Matrix Visual             │
│  Calendar Hierarchy         │          │  Report 2: Cascading Slicers         │
│  Year→Quarter→Month→Day     │          │  Report 3: Hierarchical Drill-Down   │
│  Geography Hierarchy        │          │  Report 4: Drill-Through             │
│  Region→State→City→Airport  │          │                                      │
│                             │          │  Published & live in Power BI Service│
│  Excel OLAP Operations:     │          └─────────────────────────────────────┘
│  Roll-up, Drill-Down,       │
│  Slice, Dice, Pivot         │
└─────────────────────────────┘
```
 
---
 
## 📁 Repository Structure
 
```
flight-data-warehouse/
│
├── 📂 01_source_preparation/
│   ├── notebooks/
│   │   ├── 01_dataset_evaluation.ipynb     # Null analysis, type profiling, QC checks
│   │   └── 02_normalisation_export.ipynb   # Entity extraction, FK assignment, file export
│   └── outputs/
│       ├── flight_operations.csv           # 7M rows — core transactional source
│       ├── delay_records.csv               # ~1.4M rows — delay component source
│       ├── airlines.csv                    # 15 IATA carriers → import to AirlineOLTP
│       ├── airports.csv                    # 348 airports → import to AirlineOLTP
│       ├── flights.csv                     # 600K route definitions → import to AirlineOLTP
│       ├── reference_data.xlsx             # CancellationReasons + RegionMapping sheets
│       └── reports/
│           ├── dataset_evaluation_report.csv
│           └── quality_check_results.csv
│
├── 📂 02_database/
│   ├── FlightDW_Schema.sql                 # Full DDL: all 6 DW tables + indexes
│   ├── FlightDW_Staging_Schema.sql         # Staging tables + error tables
│   ├── usp_Prepare_FlightOperations.sql    # Stored procedure used by SSIS Package 2
│   └── validation_queries.sql             # Post-load verification queries
│
├── 📂 03_ssis_etl/
│   └── FlightDW_ETL/                       # Visual Studio SSIS project root
│       ├── Master.dtsx                     # Orchestrator package
│       ├── Package1_LoadDims.dtsx          # Dimension load (SCD2, enrichment)
│       ├── Package2_LoadFact.dtsx          # Fact load (7M rows, 6 lookups)
│       ├── Package3_AccumUpdate.dtsx       # Accumulating fact update
│       └── FlightDW_ETL.sln
│
├── 📂 04_ssas_cube/
│   └── FlightDW_SSAS/                      # Visual Studio SSAS project root
│       ├── FlightDW_SSAS.sln
│       ├── Dim Date.dim
│       ├── Dim Airline.dim
│       ├── Dim Origin Airport.dim
│       ├── Dim Destination Airport.dim
│       ├── Dim Route.dim
│       ├── Dim Cancellation Reason.dim
│       ├── Flight Operations Cube.cube
│       └── bin/
│           └── Deploy.xmla                 # XMLA deployment script for SSMS
│
├── 📂 05_excel_olap/
│   └── FlightOLAP_Operations.xlsx          # 6-sheet workbook demonstrating OLAP operations
│       ├── Roll-Up
│       ├── Drill-Down
│       ├── Slice
│       ├── Dice
│       ├── Pivot
│       └── Summary Dashboard
│
├── 📂 06_powerbi/
│   └── FlightDW_Reports.pbix               # 5-page Power BI report file
│       ├── Report 1 - Matrix
│       ├── Report 2 - Slicers
│       ├── Report 3 - Drill Down
│       ├── Report 4 - Drill-Through Summary
│       └── Drill-Through - Carrier Detail
│
├── .gitignore
└── README.md
```
 
---
 
## 📊 Dataset
 
| Attribute | Detail |
|---|---|
| **Source** | US Bureau of Transportation Statistics (BTS) via Kaggle |
| **Dataset** | [hrishitpatil/flight-data-2024](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024) |
| **Authority** | Collected under 14 CFR Part 234 airline reporting obligations |
| **Records** | 7,079,081 flight operations |
| **Columns** | 35 attributes per record |
| **Period** | 1 January 2024 — 31 December 2024 (full leap year) |
| **Airlines** | 15 IATA-coded US domestic carriers |
| **Airports** | 348 unique IATA airport codes |
| **States** | 50 states + DC + Puerto Rico + US territories |
| **Cancelled** | 96,315 flights (1.36%) |
| **Delay types** | 5 BTS-defined categories: carrier, weather, NAS, security, late aircraft |
 
### Data Source Enrichment Added
The raw BTS data was enriched with two external authoritative sources during ETL:
 
- **IATA Airline Coding Directory** — carrier full names mapped to 2-letter IATA codes
- **US Census Bureau Geographic Divisions** — state → region classification enabling a 4-level geographic hierarchy not present in the source data
 
---
 
## 🔄 ETL Pipeline
 
The ETL is implemented as four SSIS packages with strict execution dependency enforced by a master orchestrator.
 
### Execution Order
```
Master.dtsx
    │
    ├─→ Package1_LoadDims.dtsx  (must complete first — dimensions before facts)
    │       ├── Extract: 3 source types (OLE DB, Excel, Flat File)
    │       ├── Transform: Data Conversion, Derived Column (distance_band)
    │       ├── SCD Type 2: Dim_Airline (carrier name history)
    │       ├── Lookup: Region enrichment join (SQL Server + Excel → Dim_Airport)
    │       └── Load: All 5 dimension tables
    │
    ├─→ Package2_LoadFact.dtsx  (runs after Package 1 succeeds)
    │       ├── Source: EXEC usp_Prepare_FlightOperations (SQL JOIN, pre-typed)
    │       ├── 6 sequential Lookup transforms → resolve all FK references
    │       ├── Conditional routing: No Match → stg_ErrorRows (audit trail)
    │       ├── Derived Column: accm_txn_create_time = fl_date
    │       └── Load: Fact_FlightOperation — 7,079,020 rows
    │
    └─→ Package3_AccumUpdate.dtsx  (incremental — runs on completion data arrival)
            ├── Source: accum_completion.csv (10% sample, 707,902 rows)
            ├── Lookup: match operation_id → fact table
            ├── No Match routing → stg_AccumErrors (500 invalid IDs captured)
            ├── OLE DB Command: UPDATE accm_txn_complete_time per row
            └── Execute SQL: Compute txn_process_time_hours = DATEDIFF(MINUTE)/60.0
```
 
### Key ETL Design Decisions
 
**Why a stored procedure instead of SSIS Sort + Merge Join:**
Sorting 7M rows entirely in SSIS memory caused buffer exhaustion resulting in ~1.4M rows loaded instead of 7M. Moving the LEFT OUTER JOIN between `stg_FlightOperation` and `stg_DelayRecord` into SQL Server (`usp_Prepare_FlightOperations`) leverages SQL Server's indexed join optimisation and eliminates the memory bottleneck.
 
**Why `accm_txn_create_time = fl_date` not `GETDATE()`:**
The dataset is historical 2024 data loaded in 2025. Using `GETDATE()` stamps a 2025 creation time against 2024 completion times, producing negative `txn_process_time_hours`. Setting creation time to the flight date preserves the business meaning of the accumulating fact pattern.
 
**Why VARCHAR(3) not CHAR(3) for cancellation codes:**
`CHAR(3)` right-pads single characters with trailing spaces — `'A'` becomes `'A  '`. The SSIS Lookup component comparing `'A'` (VARCHAR) against `'A  '` (CHAR) returns No Match for every cancelled flight, silently dropping all 96,315 rows. `VARCHAR(3)` stores values without padding, enabling exact matches.
 
### ETL Validation Results
 
| Check | Result | Status |
|---|---|---|
| Total fact rows loaded | 7,079,020 | ✅ Expected (61 rows in error table) |
| Null FK keys (any dimension) | 0 | ✅ Full referential integrity |
| Operated rows | 6,982,705 | ✅ Matches cancellation_key=0 count |
| Cancelled rows | 96,315 | ✅ Matches keys 1–4 count |
| Error rows captured | 122 | ✅ Audit trail working |
| Accumulating updates (10%) | 707,902 | ✅ Exact 10% sample |
| Invalid IDs captured in error table | 507 | ✅ Error routing confirmed |
| Negative process hours | 0 | ✅ Timeline logic correct |
| Avg process hours | 57.58 hrs | ✅ Positive, business-realistic |
 
---
 
## 🗄 Data Warehouse Design
 
### Star Schema
 
```
                    ┌─────────────┐
                    │  Dim_Date   │
                    │  PK: date_key│
                    │  Hierarchy: │
                    │  Year→Qtr   │
                    │  →Month→Day │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────▼────────────────────────┐    ┌──────────────────┐
│  Dim_Airline │    │      Fact_FlightOperation      │    │  Dim_Airport     │
│  PK:         │    │      PK: operation_key         │    │  PK: airport_key │
│  airline_key │◄───│      FK: date_key              │    │  Hierarchy:      │
│  SCD Type 2  │    │      FK: airline_key           │───►│  Region→State    │
│              │    │      FK: origin_airport_key    │    │  →City→Airport   │
└──────────────┘    │      FK: dest_airport_key  ────┼───►│  (role-playing)  │
                    │      FK: route_key             │    └──────────────────┘
┌──────────────┐    │      FK: cancellation_key      │    ┌──────────────────┐
│  Dim_Route   │◄───│                                │    │ Dim_Cancellation │
│  PK: route_key│   │  MEASURES (additive):          │◄───│ PK: cancel_key   │
│  600K routes │    │  dep_delay, arr_delay          │    │ A/B/C/D/N/A      │
│  dist_band   │    │  taxi_out, taxi_in             │    │ DOT/BTS official │
└──────────────┘    │  carrier/weather/nas/          │    └──────────────────┘
                    │  security/late_aircraft_delay  │
                    │  actual_elapsed_time, air_time │
                    │  distance, total_delay_minutes │
                    │                                │
                    │  SEMI-ADDITIVE: cancelled,     │
                    │  diverted                      │
                    │                                │
                    │  ACCUMULATING (Task 6):        │
                    │  accm_txn_create_time          │
                    │  accm_txn_complete_time        │
                    │  txn_process_time_hours        │
                    └────────────────────────────────┘
```
 
### Slowly Changing Dimension (Type 2) — Dim_Airline
 
SCD Type 2 is applied to `Dim_Airline` to track carrier name changes over time. Airlines merge, rebrand, and enter bankruptcy (Spirit Airlines NK filed Chapter 11 in November 2024). Three tracking columns are added: `scd_effective_date`, `scd_expiry_date`, `is_current_record`. Historical operations correctly attribute flights to the carrier name that existed at the time of the flight.
 
### Accumulating Fact Pattern
 
The three accumulating columns model the post-flight reporting process — a genuine airline operations KPI. The row is inserted with `accm_txn_create_time = fl_date` and `accm_txn_complete_time = NULL`. A separate ETL package updates the completion time when downstream processing finishes (1–5 days later depending on flight type) and computes `txn_process_time_hours`.
 
---
 
## 🧊 SSAS Cube & OLAP Operations
 
### Cube Configuration
 
| Component | Detail |
|---|---|
| **Cube name** | Flight Operations Cube |
| **Database** | FlightDW_SSAS |
| **Measure group** | Fact_FlightOperation |
| **Dimensions** | 6 (Date, Airline, Origin Airport, Destination Airport, Route, Cancellation Reason) |
| **Hierarchies** | Calendar (Year→Quarter→Month→Day), Geography (Region→State→City→Airport) |
| **Measures** | 16 (delay by 5 types, elapsed time, air time, distance, counts, process hours) |
| **Deployment** | XMLA script via SSMS (VS extension client library bypass for SQL Server 2022) |
 
### Deployment Note
 
The Visual Studio Analysis Services Projects extension bundles its own internal AMO libraries which are outdated relative to SQL Server 2022 SSAS (`StandardDeveloper64` edition). The solution builds the `.asdatabase` file in Visual Studio, then converts it to XMLA via `Microsoft.AnalysisServices.Deployment.exe` and executes it directly in SSMS — which uses the correct SQL Server 2022 native libraries.
 
### OLAP Operations Demonstrated in Excel
 
| Operation | Definition | Implementation |
|---|---|---|
| **Roll-Up** | Aggregate from lower to higher hierarchy level | Calendar hierarchy collapsed from Day → Month → Quarter → Year |
| **Drill-Down** | Navigate from summary to detail level | Calendar + Geography cross-section expanded to daily granularity |
| **Slice** | Fix one dimension at a single value | Carrier slicer filtering entire cube to one airline |
| **Dice** | Constrain two or more dimensions simultaneously | Three slicers active: Carrier + Quarter + Distance Band |
| **Pivot** | Rotate row/column axes for different analytical perspective | Swap carrier_name and quarter_name between Rows/Columns |
 
---
 
## 📈 Power BI Reports
 
All four reports are published to Power BI Service and accessible online.
 
### Report 1 — Matrix Visual
Airline performance matrix with hierarchical row groupings (Region → Carrier) and quarterly column groupings. Conditional formatting on On-Time Rate % (red → green gradient). Includes row and column subtotals.
 
### Report 2 — Cascading Slicers
Four interactive slicers with cascading behaviour: selecting a **Region** automatically limits the **State** slicer to states within that region. Selecting a **State** further filters a bar chart (delays by carrier), a line chart (monthly on-time trend), and a donut chart (delay causes). Three KPI Cards update dynamically.
 
**Cascading mechanism:** Both the Region and State slicers reference `Dim_Airport` — Power BI automatically propagates filter context through the model when slicers share a table, requiring no DAX configuration.
 
### Report 3 — Hierarchical Drill-Down
Column chart and line chart both bound to the **Calendar Hierarchy** (`Dim_Date`). Drill-down arrows allow navigation from Year → Quarter → Month → Day. A context-sensitive table and three KPI cards update at each level to show totals for the currently selected time period.
 
### Report 4 — Drill-Through
**Summary page:** Carrier ranking bar chart, performance matrix, and scatter chart (delay vs cancellation rate, bubble size = flight volume).
 
**Detail page:** Configured as a drill-through destination on `carrier_name`. Right-clicking any carrier in the summary page navigates to a detail page showing: monthly performance trend, delay breakdown by cause per quarter, top 10 routes by delay, and a KPI gauge against the 85% on-time industry benchmark. Auto-generated Back button returns to summary.
 
### DAX Measures
 
```dax
Total Flights = COUNTROWS(Fact_FlightOperation)
 
On-Time Rate % =
DIVIDE(
    CALCULATE(
        COUNTROWS(Fact_FlightOperation),
        Fact_FlightOperation[cancelled] = 0,
        Fact_FlightOperation[arr_delay] <= 0
    ),
    CALCULATE(
        COUNTROWS(Fact_FlightOperation),
        Fact_FlightOperation[cancelled] = 0
    ), 0
) * 100
 
Cancellation Rate % = DIVIDE([Cancelled Flights], [Total Flights], 0) * 100
 
Avg Arrival Delay =
CALCULATE(
    AVERAGE(Fact_FlightOperation[arr_delay]),
    Fact_FlightOperation[cancelled] = 0
)
```
 
---
 
## 🛠 Tech Stack
 
| Layer | Technology | Purpose |
|---|---|---|
| Data Profiling | Python 3.11 (pandas, openpyxl) | Dataset evaluation, normalisation, source file export |
| Cloud Notebook | Google Colab | Kaggle API download, 7M-row processing, file generation |
| Relational DB | SQL Server 2022 | FlightDW warehouse + FlightDW_Staging databases |
| ETL | SSIS (SQL Server Integration Services) | 3-package ETL pipeline with SCD, Lookups, error routing |
| OLAP | SSAS (SQL Server Analysis Services) | Multidimensional cube with 2 hierarchies |
| BI Authoring | Power BI Desktop | Report design, DAX measures, data modelling |
| BI Publishing | Power BI Service | Cloud report hosting and sharing |
| Excel | Microsoft Excel | OLAP analysis connected to SSAS cube |
| IDE | Visual Studio 2022 | SSIS + SSAS project development |
| DB IDE | SSMS 2022 | SQL queries, XMLA execution, cube browsing |
 
---
 
## 🚀 Getting Started

This project includes reproducible setup instructions for restoring the FlightDW database, deploying the SSAS cube, and opening the Excel and Power BI reports.  
For full technical details, see [INSTALL.md](./INSTALL.md).

---
 
## ⚠️ Known Issues & Solutions
 
**SSAS Deployment: `StandardDeveloper64 edition not supported`**
> When deploying SSAS projects with SQL Server 2022, Visual Studio’s Analysis Services Projects extension may bundle older AMO libraries that sometimes cause compatibility issues. In practice, the **SQL Server 2022 Developer Edition** works correctly for SSAS deployment. If you encounter errors with the built‑in Deploy button in Visual Studio, try connecting directly through SSMS to process the cube.
 
**SSIS Partial Load (~1.4M rows instead of 7M)**
> SSIS Sort + Merge Join components exhaust memory buffers on 7M rows. The stored procedure `usp_Prepare_FlightOperations` (in `02_database/`) pre-joins the tables in SQL Server and outputs a single typed result set — eliminating the need for Sort/Merge Join components entirely.
 
**Cancelled flights not loading (CHAR/VARCHAR mismatch)**
> `CHAR(3)` right-pads single characters: `'A'` → `'A  '`. The dimension was recreated with `VARCHAR(3)` to prevent padding. If this recurs, run `SELECT cancellation_code, LEN(cancellation_code) FROM Dim_CancellationReason` — all codes should return their natural length (3 for N/A, 1 for A/B/C/D).
 
**Power BI file too large to publish**
> With 7M rows imported, the `.pbix` may exceed 1GB. Use Power BI Live Connection to SSAS instead: Get Data → Analysis Services → connect in Live Connection mode. The file remains small and queries are served by the cube at runtime.
 
---
 
## 📐 Data Model Reference
 
### Source Entities (OLTP Normalised Model)
 
| Entity | Rows | Natural Key | Source Authority |
|---|---|---|---|
| Airline | 15 | carrier_code (IATA) | IATA Airline Coding Directory |
| Airport | 348 | airport_code (IATA) | BTS source data |
| Flight | 600,316 | carrier+flightno+origin+dest+times | Derived from BTS records |
| FlightOperation | 7,079,081 | operation_id | BTS 14 CFR Part 234 |
| DelayRecord | ~1,400,000 | delay_id | BTS Form 41 |
| CancellationReason | 4 | cancellation_code | DOT/BTS Form 41 data dictionary |
 
### Warehouse Tables
 
| Table | Rows | Type | Key Design Feature |
|---|---|---|---|
| Dim_Date | 366 | Dimension | Calendar hierarchy, holiday flags |
| Dim_Airline | 60 | Dimension (SCD2) | 3 tracking columns, current flag |
| Dim_Airport | 348 | Dimension (role-playing) | Region enrichment, Geography hierarchy |
| Dim_Route | 600,316 | Dimension | Derived distance_band attribute |
| Dim_CancellationReason | 5 | Dimension | Default N/A row eliminates NULL FKs |
| Fact_FlightOperation | 7,079,020 | Fact (accumulating) | 15 additive + 3 accumulating measures |
 
---
 
## 🎓 Academic Context
 
This project was developed as a two-part academic assignment for the **Data Warehouse & Business Intelligence** module at the **Sri Lanka Institute of Information Technology (SLIIT)**, Faculty of Computing.
While academic in origin, the implementation was carried out with professional standards in mind — including schema design in SSMS, ETL workflows in SSIS, cube modeling in SSAS, and reporting in Power BI and Excel. The goal was not only to meet assignment requirements but also to practice end‑to‑end BI development as it would be done in industry.
 
---
 
## 📄 License
 
This project is licensed under the MIT License. The dataset is sourced from the US Bureau of Transportation Statistics and is in the public domain under US government open data policy.
 
---
 
<div align="center">
 
**Built with SQL Server · SSIS · SSAS · Power BI · Python**
 
*7 million rows → one analytical truth*
 
</div>
