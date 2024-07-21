# E-Commerce Data Analysis

This project involves an ETL (Extract, Transform, Load) pipeline for processing e-commerce data using Apache Spark. The application reads raw data from a CSV file, processes it to generate various statistics and aggregations, and writes the results back to CSV files.


## ETL Pipeline Components

### DataProcess.scala
Contains functions for data cleaning and transformation, including:
- `processData`: Cleans the data and computes various statistics and aggregations.
- `calculateMonthlySales`: Computes monthly sales and other related statistics.

### DataReader.scala
Contains the function for reading raw data from a CSV file into a Spark DataFrame.

### ETLApp.scala
The main application that orchestrates the ETL process. It reads raw data, processes it, computes monthly sales, and writes the results to CSV files.

## Usage

### Prerequisites

- [Apache Spark](https://spark.apache.org/downloads.html)
- [Hadoop](https://hadoop.apache.org/releases.html)
- [Scala](https://www.scala-lang.org/download/)
- [sbt (Scala Build Tool)](https://www.scala-sbt.org/download.html)

### Running the Application

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/ecommerce-data-analysis.git
    cd ecommerce-data-analysis
    ```

2. Place your raw e-commerce data file (`ECommerceDataset.csv`) in the `data/raw` directory.

3. Build the project using sbt:
    ```sh
    sbt clean compile
    ```

4. Run the ETL application:
    ```sh
    sbt run
    ```

### Output

The processed data and monthly sales report will be saved in the `data/output` directory as CSV files:
- `ECommerceDataset.csv`
- `Monthly_Sales_Report.csv`

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For any inquiries or feedback, please reach out to [your-email@example.com](mailto:your-email@example.com).
