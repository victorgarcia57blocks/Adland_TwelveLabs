# Adland to TwelveLabs Metadata Migration

This repository contains the ETL (Extract, Transform, Load) pipeline developed to migrate the legacy Adland video library (~64k assets) from JW Player to the TwelveLabs ingestion format.

## 🚀 Overview

The goal of this project was to generate a relational metadata structure (`Manifest`, `Tags`, `Video_Tags`) while overcoming API pagination limits inherent in large historical datasets.

### Key Strategies Implemented:
* **ID Sharding Extraction:** Instead of time-based pagination (which failed due to high data density in migration months), the harvester iterates through alphanumeric ID prefixes (0-9, a-z, A-Z). This ensures 100% data capture without hitting API limits.
* **Relational Normalization:** Raw tag strings are processed into a normalized schema to optimize database performance.
* **Data Curation:** Snippets are truncated to 200 characters and sanitized.

## 📂 Repository Structure

```text
Adland_TwelveLabs/
│
├── src/                     
│   ├── master_harvester.py   # Extracts raw data from JW Player API
│   └── splitter.py           # Normalizes data into 3 CSV deliverables
│
├── requirements.txt          # Python dependencies
└── README.md                 # Documentation
🛠️ Setup & Usage
1. Prerequisites
Python 3.9+

JW Player API Credentials

2. Installation
Bash

pip install -r requirements.txt
3. Execution
Step 1: Extract Data (Harvester)
This script generates the TwelveLabs_Master_Manifest.csv.
Note: Set your API Secret as an environment variable before running.

Bash

# Set your API Secret first
export JW_API_SECRET="your_secret_key_here" 

# Run the script
python src/master_harvester.py
Step 2: Transform Data (Splitter)
This script reads the master manifest and generates the final deliverables.

Bash

python src/splitter.py
📊 Outputs
The pipeline generates three files compliant with the ingestion spec:

Video_manifest.csv: Contains Unique ID, Filename (GCS mapped), Snippet, etc.

Tags.csv: Unique list of tags with assigned IDs.

Video_tags.csv: Bridge table linking Videos to Tags.