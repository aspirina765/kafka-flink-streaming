const { writeFile, write } = require('fs');

const path = './config.json';
const config = { ip: '192.0.2.10', port: 3000 };

const writeFiles = async () => writeFile(path, JSON.stringify(config, null, 2), (error) => {
  if (error) {
    console.log('An error has occurred ', error);
    return;
  }
  console.log('Data written successfully to disk');
});

// writeFiles();

module.exports = writeFiles;