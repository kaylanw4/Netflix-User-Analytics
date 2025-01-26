const express = require('express');
const path = require('path');
const mysql = require('mysql2');
const { exec } = require('child_process');
const fileUpload = require('express-fileupload');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

const connection = mysql.createConnection({
    host: '35.238.150.143',
    user: 'root',
    password: 'team117',
    database: 'netflix_wrapped'
});

connection.connect(function(err) {
    if (err) {
        console.error('Error connecting to database:', err);
        return;
    }
    console.log('Connected to MySQL database');
});

app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

// Serve static files from the 'public' directory
app.use(express.static(path.join(__dirname, 'views')));
app.use('/css', express.static(path.join(__dirname, 'views/css')));

app.use(fileUpload());

app.get('/', function(req, res) {
    res.render('index');
});

app.get('/results', function(req, res) {
    res.render('results');
});

app.get('/analysis', function(req, res) {
    res.render('analysis'); // Render the analysis.ejs template
});

app.post('/api/upload-csv', function(req, res) {
    if (!req.files || Object.keys(req.files).length === 0) {
        return res.status(400).send('No files were uploaded.');
    }

    const csvFile = req.files.csvFile;
    const uploadDir = path.join(__dirname, 'tmp');
    const filePath = path.join(uploadDir, csvFile.name);

    csvFile.mv(filePath, function(err) {
        if (err) {
            return res.status(500).send(err);
        }

        const pythonScript = `python3 your_python_script.py ${filePath}`;
        exec(pythonScript, (error, stdout, stderr) => {
            if (error) {
                console.error(`Error executing Python script: ${error.message}`);
                return res.status(500).send({ message: 'Error executing Python script', error: error });
            }
            if (stderr) {
                console.error(`Python script stderr: ${stderr}`);
            }
            res.redirect('/results');
        });
    });
});

app.get('/api/display', function(req, res) {
    var sql = 'SELECT * FROM display';
    connection.query(sql, function(err, results) {
        if (err) {
            console.error('Error fetching display info:', err);
            return res.status(500).send({ message: 'Error fetching display info', error: err });
        }
        res.json(results);
    });
});

// Endpoint to add a new display item with transaction
app.post('/api/display/add', function(req, res) {
    const { id,tconst, primaryTitle, runtimeMinutes, Season, Date, titleType } = req.body;

    // Begin a transaction
    const startTransactionQuery = `START TRANSACTION`;
    connection.query(startTransactionQuery, function(err) {
        if (err) {
            console.error('Error beginning transaction:', err);
            return res.status(500).send({ message: 'Error beginning transaction', error: err });
        }

        const sql = 'INSERT INTO display (id, tconst, primaryTitle, runtimeMinutes, Season, Date, titleType) VALUES (?, ?, ?, ?, ?, ?, ?)';
        connection.query(sql, [id, tconst, primaryTitle, runtimeMinutes, Season, Date, titleType], function(err, result) {
            if (err) {
                console.error('Error adding to display:', err);

                // Rollback the transaction if there's an error
                const rollbackQuery = `ROLLBACK`;
                connection.query(rollbackQuery, function(rollbackErr) {
                    if (rollbackErr) {
                        console.error('Error rolling back transaction:', rollbackErr);
                    }
                    console.error('Transaction rolled back.');
                    return res.status(500).send({ message: 'Error adding to display', error: err });
                });
            }

            // Commit the transaction if the query is successful
            const commitQuery = `COMMIT`;
            connection.query(commitQuery, function(commitErr) {
                if (commitErr) {
                    console.error('Error committing transaction:', commitErr);
                    return res.status(500).send({ message: 'Error committing transaction', error: commitErr });
                }
                console.log('Transaction committed successfully.');
                res.send({ message: 'Data added to display successfully' });
            });
        });
    });
});

// Endpoint to update a display item with transaction
app.post('/api/display/update/:id', function(req, res) {
    const displayId = req.params.id;
    const { primaryTitle, runtimeMinutes, Season, Date, titleType } = req.body;

    // Begin a transaction
    const startTransactionQuery = `START TRANSACTION`;
    connection.query(startTransactionQuery, function(err) {
        if (err) {
            console.error('Error beginning transaction:', err);
            return res.status(500).send({ message: 'Error beginning transaction', error: err });
        }

        const sql = 'UPDATE display SET primaryTitle = ?, runtimeMinutes = ?, Season = ?, Date = ?, titleType = ? WHERE id = ?';
        connection.query(sql, [primaryTitle, runtimeMinutes, Season, Date, titleType, displayId], function(err, result) {
            if (err) {
                console.error('Error updating display:', err);

                // Rollback the transaction if there's an error
                const rollbackQuery = `ROLLBACK`;
                connection.query(rollbackQuery, function(rollbackErr) {
                    if (rollbackErr) {
                        console.error('Error rolling back transaction:', rollbackErr);
                    }
                    console.error('Transaction rolled back.');
                    return res.status(500).send({ message: 'Error updating display', error: err });
                });
            }

            // Commit the transaction if the query is successful
            const commitQuery = `COMMIT`;
            connection.query(commitQuery, function(commitErr) {
                if (commitErr) {
                    console.error('Error committing transaction:', commitErr);
                    return res.status(500).send({ message: 'Error committing transaction', error: commitErr });
                }
                console.log('Transaction committed successfully.');
                res.send({ message: 'Display info updated successfully' });
            });
        });
    });
});

// Endpoint to delete a display item with transaction
app.post('/api/display/delete/:id', function(req, res) {
    const displayId = req.params.id;
    const sql = 'DELETE FROM display WHERE id = ?';

    // Begin a transaction
    const startTransactionQuery = `START TRANSACTION`;
    connection.query(startTransactionQuery, function(err) {
        if (err) {
            console.error('Error beginning transaction:', err);
            return res.status(500).send({ message: 'Error beginning transaction', error: err });
        }

        connection.query(sql, [displayId], function(err, result) {
            if (err) {
                console.error('Error deleting display info:', err);

                // Rollback the transaction if there's an error
                const rollbackQuery = `ROLLBACK`;
                connection.query(rollbackQuery, function(rollbackErr) {
                    if (rollbackErr) {
                        console.error('Error rolling back transaction:', rollbackErr);
                    }
                    console.error('Transaction rolled back.');
                    return res.status(500).send({ message: 'Error deleting display info', error: err });
                });
            }

            // Commit the transaction if the query is successful
            const commitQuery = `COMMIT`;
            connection.query(commitQuery, function(commitErr) {
                if (commitErr) {
                    console.error('Error committing transaction:', commitErr);
                    return res.status(500).send({ message: 'Error committing transaction', error: commitErr });
                }
                console.log('Transaction committed successfully.');
                res.send({ message: 'Display info deleted successfully' });
            });
        });
    });
});


app.get('/api/runtime', function(req, res) {
    calculateRuntime(res);
});

function calculateRuntime(response) {
    var sql = `
    SELECT CONCAT('20', SUBSTRING(d.Date, -2)) as year, SUM(d.runtimeMinutes) as totaltime, d.titleType
    FROM display d
    WHERE d.titleType = 'movie'
    GROUP BY year, d.titleType
    UNION
    SELECT CONCAT('20', SUBSTRING(d.Date, -2)) as year, SUM(d.runtimeMinutes) as totaltime, d.titleType
    FROM display d
    WHERE d.titleType != 'movie'
    GROUP BY year, d.titleType
    ORDER BY year DESC;
    
    `;

    connection.query(sql, function(err, results) {
        if (err) {
            console.error('Error fetching total runtime data:', err);
            response.status(500).send({ message: 'Error fetching total runtime data', error: err });
        } else {
            response.json(results);
        }
    });
}

app.get('/api/binged_tv', function(req, res) {
    calculateBingedTV(res);
});

function calculateBingedTV(response) {
    var sql = `
    SELECT d.primarytitle, sum(d.runtimeMinutes) as totaltime, (sum(d.runtimeMinutes)/(d.runtimeMinutes)) as number_of_eps
    FROM display d
    WHERE d.titleType != 'movie'
    GROUP BY d.primarytitle,  d.runtimeMinutes
    ORDER BY totaltime DESC
    LIMIT 15;
    `;

    connection.query(sql, function(err, results) {
        if (err) {
            console.error('Error fetching binged TV data:', err);
            response.status(500).send({ message: 'Error fetching binged TV data', error: err });
        } else {
            response.json(results);
        }
    });
}

app.get('/api/genre', function(req, res) {
    calculategenre(res);
});

function calculategenre(response) {
    var sql = `
    SELECT generes, COUNT(*) AS GenreCount
    FROM display d
    JOIN title_to_genres tg on tg.tconst = d.tconst
    JOIN genres g ON tg.id = g.id
    WHERE d.titleType = 'movie'
    GROUP BY generes
    ORDER BY GenreCount DESC;
    `;

    connection.query(sql, function(err, results) {
        if (err) {
            console.error('Error fetching genre data:', err);
            response.status(500).send({ message: 'Error fetching genre data', error: err });
        } else {
            response.json(results);
        }
    });
}


app.get('/api/director', function(req, res) {
    calculatedirector(res);
});

function calculatedirector(response) {
    var sql = `
    SELECT c.primaryName, COUNT(*) AS WatchCount
    FROM display d
    JOIN title_to_crew tg on tg.titleconst = d.tconst
    JOIN crew c ON tg.directors = c.nconst
    WHERE d.titleType = 'movie'
    GROUP BY c.nconst
    ORDER BY WatchCount DESC
    LIMIT 15;
    `;

    connection.query(sql, function(err, results) {
        if (err) {
            console.error('Error fetching director data:', err);
            response.status(500).send({ message: 'Error fetching director data', error: err });
        } else {
            response.json(results);
        }
    });
}



// New API endpoint
app.get('/api/create/:year', function(req, res) {
    const selectedYear = req.params.year;

    // Sample SQL query
    const query = `CALL analysis_create('${selectedYear}')`;

    // Execute the query
    connection.query(query, function(err, results) {
        if (err) {
            console.error('Error executing query:', err);
            return res.status(500).send({ message: 'Error executing query', error: err });
        }
        res.json(results);
    });
});

// Route to handle search and filter display items
app.post('/api/display/search', function(req, res) {
    const searchText = req.body.searchText;
    const sql = `SELECT * FROM display WHERE primaryTitle LIKE '%${searchText}%'`;
    connection.query(sql, function(err, results) {
        if (err) {
            console.error('Error fetching filtered display info:', err);
            return res.status(500).send({ message: 'Error fetching filtered display info', error: err });
        }
        res.json(results);
    });
});

app.listen(80, function () {
    console.log('Node app is running on port 80');
});
