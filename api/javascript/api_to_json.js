const https = require('https');
const fs = require('fs');

const url = 'https://randomuser.me/api/';
const filePath = 'data.json';

https.get(url, (response) => {
  let body = '';

  response.on('data', (chunk) => {
    body += chunk;
  });

  response.on('end', () => {
    fs.writeFile(filePath, body, (err) => {
      if (err) {
        console.error('Error writing file:', err);
      } else {
        console.log('File saved successfully.');
      }
    });
  });
}).on('error', (err) => {
  console.error('Error requesting URL:', err.message);
});
