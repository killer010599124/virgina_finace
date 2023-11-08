const express = require("express");
const mysql = require("mysql");
const fs = require("fs");
const csv = require("csv-parser");
const cors = require("cors");
const { log } = require("console");
// Create a MySQL connection
const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "",
  database: "virgina-org",
});

const csvFilePath = "ScheduleI.csv";
const tableName = "scheduleI";

const uniqueColumn = "id";

// Connect to the MySQL server
connection.connect((err) => {
  if (err) {
    console.error("Error connecting to the database: " + err.stack);
    return;
  }
  console.log("Connected to the database as ID " + connection.threadId);
  // importData();
});

// Create an Express application
const app = express();
app.use(cors());
app.use(express.json());
// Define a route to perform a database query
app.get("/getData", (req, res) => {
  // Retrieve the 'param1' and 'param2' GET parameters
  const tablename = req.query.tablename;
  const name = req.query.search1;
  const org = req.query.search2;
  const amountFrom = req.query.search3;
  const amountTo = req.query.search4;
  const dateFrom = req.query.search5;
  const dateTo = req.query.search6;
  const zip = req.query.search7;
  const city = req.query.search8;

  let query = `SELECT * FROM ${tablename} WHERE `;
  const conditions = [];

  if(tablename === "ScheduleA" || tablename === "ScheduleB"){
    if (name) {
      conditions.push(`FirstName = '${name}'`);
    }
  
    if (org) {
      conditions.push(`LastOrCompanyName = '${org}'`);
    }
  
    if (amountFrom && amountTo) {
      conditions.push(`Amount BETWEEN ${amountFrom} AND ${amountTo}`);
    }
    if (dateFrom && dateTo) {
      const escapedDateFrom = formatDateForDatabase(dateFrom);
      const escapedDateTo = formatDateForDatabase(dateTo);
      conditions.push(
        `STR_TO_DATE(TransactionDate, '%d/%m/%Y') >= STR_TO_DATE('${escapedDateFrom}', '%Y/%m/%d') AND STR_TO_DATE(TransactionDate, '%d/%m/%Y') <= STR_TO_DATE('${escapedDateTo}', '%Y/%m/%d')`
      );
    }
    if (city) {
      conditions.push(`PrimaryCityAndStateOfEmploymentOrBusiness = '${city}'`);
    }
  
    if (zip) {
      conditions.push(`ZipCode = '${zip}'`);
    }
  
    query += conditions.join(" AND ");
  }
  else if(tablename === "ScheduleC" || tablename === "ScheduleD" || tablename === "ScheduleF" || tablename === "ScheduleI"){
    if (name) {
      conditions.push(`FirstName = '${name}'`);
    }
  
    if (org) {
      conditions.push(`LastOrCompanyName = '${org}'`);
    }
  
    if (amountFrom && amountTo) {
      conditions.push(`Amount BETWEEN ${amountFrom} AND ${amountTo}`);
    }
    if (dateFrom && dateTo) {
      const escapedDateFrom = formatDateForDatabase(dateFrom);
      const escapedDateTo = formatDateForDatabase(dateTo);
      conditions.push(
        `STR_TO_DATE(TransactionDate, '%d/%m/%Y') >= STR_TO_DATE('${escapedDateFrom}', '%Y/%m/%d') AND STR_TO_DATE(TransactionDate, '%d/%m/%Y') <= STR_TO_DATE('${escapedDateTo}', '%Y/%m/%d')`
      );
    }
  
    if (zip) {
      conditions.push(`ZipCode = '${zip}'`);
    }
  
    query += conditions.join(" AND ");
  }
  else if(tablename === "ScheduleE"){
    if (name) {
      conditions.push(`LenderFirstName = '${name}'`);
    }
  
    if (org) {
      conditions.push(`LenderLastOrCompanyName = '${org}'`);
    }
  
    if (amountFrom && amountTo) {
      conditions.push(`Amount BETWEEN ${amountFrom} AND ${amountTo}`);
    }
    if (dateFrom && dateTo) {
      const escapedDateFrom = formatDateForDatabase(dateFrom);
      const escapedDateTo = formatDateForDatabase(dateTo);
      conditions.push(
        `STR_TO_DATE(TransactionDate, '%d/%m/%Y') >= STR_TO_DATE('${escapedDateFrom}', '%Y/%m/%d') AND STR_TO_DATE(TransactionDate, '%d/%m/%Y') <= STR_TO_DATE('${escapedDateTo}', '%Y/%m/%d')`
      );
    }
  
    if (zip) {
      conditions.push(`LenderZipCode = '${zip}'`);
    }
  
    query += conditions.join(" AND ");
  }
  else if(tablename === "ScheduleG"){
   
    if (name && org) {
      conditions.push(`ScheduleATotal BETWEEN ${name} AND ${org}`);
    }
    if (amountFrom && amountTo) {
      conditions.push(`ScheduleBTotal BETWEEN ${amountFrom} AND ${amountTo}`);
    }
    if (dateFrom && dateTo) {
      conditions.push(`ScheduleCTotal BETWEEN ${dateFrom} AND ${dateTo}`);
    }
    if (zip && city) {
      conditions.push(`ScheduleDTotal BETWEEN ${zip} AND ${city}`);
    }
   
  
    query += conditions.join(" AND ");
  }
  else if(tablename === "ScheduleH"){
   
    if (name && org) {
      conditions.push(`BeginningBalance BETWEEN ${name} AND ${org}`);
    }
    if (amountFrom && amountTo) {
      conditions.push(`ExpendableFundsBalance BETWEEN ${amountFrom} AND ${amountTo}`);
    }
    if (dateFrom && dateTo) {
      conditions.push(`TotalFundsAvailable BETWEEN ${dateFrom} AND ${dateTo}`);
    }
    if (zip && city) {
      conditions.push(`	EndingBalance BETWEEN ${zip} AND ${city}`);
    }
   
  
    query += conditions.join(" AND ");
  }


  log(query);

  connection.query(query, (err, results) => {
    if (err) {
      console.error("Error executing the query: " + err.stack);
      res.status(500).json({ error: "Database error" });
      return;
    }

    // Send the query results as JSON
    res.json(results);
  });
});

// Start the server
const port = 3000;
app.listen(port, () => {
  console.log("Server listening on port " + port);
});

function formatDateForDatabase(dateString) {
  const dateParts = dateString.split("/");
  const day = dateParts[0];
  const month = dateParts[1];
  const year = dateParts[2];
  return `${year}/${month}/${day}`;
}
function createTable(header) {
  // Modify the following code based on your CSV file structure and desired table schema
  const columns = header.map((column) => `${column} VARCHAR(255)`).join(", ");
  const query = `CREATE TABLE IF NOT EXISTS ${tableName} (${columns})`;

  connection.query(query, (error, results, fields) => {
    if (error) {
      console.error("Error creating table:", error);
    }
  });
}

function insertDataBatch(rows, callback) {
  const header = Object.keys(rows[0]);
  const values = rows.map((row) => header.map((column) => row[column]));

  const query = `INSERT IGNORE INTO ${tableName} (${header.join(
    ", "
  )}) VALUES ?`;
console.log(query)
  connection.query(query, [values], (error, results, fields) => {
    if (error) {
      console.error("Error inserting data:", error);
    }
    callback();
  });
}

function importData() {
  let header;
  let rows = [];
  let rowCount = 0;
  let batchCount = 0;

  fs.createReadStream(csvFilePath)
    .pipe(csv())
    .on("headers", (headers) => {
      header = headers;
      createTable(header);
    })
    .on("data", (row) => {
      rows.push(row);
      rowCount++;

      // Adjust the batch size based on your system's memory capacity
      const batchSize = 1000;

      // Insert data in batches
      if (rowCount === batchSize) {
        rowCount = 0;
        batchCount++;

        insertDataBatch(rows, () => {
          console.log(`Batch ${batchCount} inserted.`);
          rows = [];
        });
      }
    })
    .on("end", () => {
      // Insert any remaining rows
      if (rowCount > 0) {
        insertDataBatch(rows, () => {
          console.log(`Final batch inserted.`);
        });
      }

      console.log("Data imported successfully.");

      // Close the database connection
      connection.end();
    });
}
