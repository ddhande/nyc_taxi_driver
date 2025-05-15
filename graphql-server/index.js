require('dotenv').config()
const {ApolloServer} = require('apollo-server')
const fs = require('fs');
const path = require('path');
const resolvers = require('./resolvers');

// Schema file ko read karo
const typeDefs = fs.readFileSync(
  path.join(__dirname, 'schema.graphql'),
  'utf8'
);

// Apollo Server setup
const server = new ApolloServer({
  typeDefs,
  resolvers,
});

// Server start karo
server.listen().then(({ url }) => {
  console.log(`Server ready at ${url}`);
});