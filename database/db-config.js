const { Pool } = require('pg');

const pool = new Pool({
  user:process.env.db_user,
  host:process.env.db_host,
  database:process.env.database,
  password:process.env.db_password,
  port: 5432,
});

module.exports= {pool}