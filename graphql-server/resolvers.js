const { exec } = require('child_process');
const {pool} = require('../database/db-config')
const path = require('path');

const resolvers = {
  Query: {
    getDailyTrips: async () => {
      const res = await pool.query('SELECT * FROM daily_trips');
      return res.rows.map(row => ({
        date: row.date.toISOString().split('T')[0],
        tripDistance: row.trip_distance,
        fareAmount: row.fare_amount,
        passengerCount: row.passenger_count,
      }));
    },
  },
 Mutation: {
    processTaxiData: (_, { filePath }) => {
      console.log('Triggering Python script with filePath:', filePath);
      console.log('Current working directory:', path.join(__dirname, '..'));
      console.log('Python script path:', path.join(__dirname, '..', 'python-scripts', 'transform.py'));
      return new Promise((resolve, reject) => {
        // Use virtual environment's Python
        const pythonPath = path.join(__dirname, '..', 'venv', 'Scripts', 'python.exe');
        const scriptPath = path.join(__dirname, '..', 'python-scripts', 'transform.py');
        const command = `"${pythonPath}" "${scriptPath}" "${filePath}"`;
        console.log('Executing command:', command);
        exec(command, { cwd: path.join(__dirname, '..') }, (err, stdout, stderr) => {
          if (err) {
            console.error('Execution Error:', err);
            console.error('STDERR:', stderr);
            reject(new Error(`Processing failed: ${stderr || err.message}`));
          } else {
            console.log('STDOUT:', stdout);
            resolve(stdout);
          }
        });
      });
    },
  },
};
module.exports = resolvers;