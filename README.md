# youtube-data-harvesting
Introduction
This README file provides an overview and guidelines for harvesting data from YouTube and warehousing it effectively for analysis and other purposes.

Purpose
The purpose of this project is to collect data from YouTube, including videos, comments, metadata, and other relevant information, and store it in a structured manner for further analysis, insights generation, and potential application development.

Prerequisites
Python 3.x
Google Developer Account
YouTube Data API Key

Libraries: google-api-python-client, pandas, streamlit.
Setup

Enable YouTube Data API: In the Google Developers Console, enable the YouTube Data API for your project.

Obtain API Key: Generate an API key from the Google Developers Console. This key will be used to authenticate your requests to the YouTube Data API.

Harvesting Data
Select Data to Harvest: Determine what type of data you want to harvest from YouTube. This could include videos, comments, channel information, etc.

Use YouTube Data API: Utilize the YouTube Data API to retrieve the desired data. Refer to the YouTube Data API documentation for available endpoints and parameters.

Python Script: Write Python scripts to interact with the YouTube Data API. Use libraries like google-api-python-client to make requests and retrieve data.

echnologies Used:

Python

Google API Client Library

MongoDB

PostgreSQL

Streamlit

Data Warehousing
Data Storage: Decide on the storage method for the harvested data. Options include relational databases (MySQL, PostgreSQL).

Data Schema: Define a schema for storing the data. This could involve creating tables/collections with appropriate fields to store the retrieved information.

ETL Process: Develop an ETL (Extract, Transform, Load) process to transfer the harvested data into the chosen storage solution. This may involve data cleaning, transformation, and normalization.

Automation: Implement automation for regular data harvesting and warehousing tasks. Schedule scripts to run at specified intervals for continuous data collection and storage.
