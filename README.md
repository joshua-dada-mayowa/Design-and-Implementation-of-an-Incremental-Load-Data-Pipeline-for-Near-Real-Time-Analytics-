A comprehensive incremental load data pipeline project that extracts orders, reviews, and shipment data from an Amazon S3 bucket daily. The project transforms and loads the data into fact tables in a Postgres database, and creates analytics tables to store aggregated data, including:

- Best-performing products
- Public holiday data
- Shipment data

The project also populates the analytics tables with data, enabling further data visualization and insights. As an incremental load pipeline, it efficiently processes only new and updated data, minimizing data duplication and reducing processing time.
