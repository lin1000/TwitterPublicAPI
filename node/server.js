
var emptygif = require('emptygif');
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);
var cookieParser = require('cookie-parser')

result =  require('dotenv').config()
console.log(result.parsed)


// set the view engine to ejs
app.set('view engine', 'ejs');

var ejs = require('ejs'),
  people = ['geddy', 'neil', 'alex'],
  html = ejs.render('<%= people.join(", "); %>', { people: people });

app.use(cookieParser())

// index page 
app.get('/', function (req, res) {

  //Get Mapbox API Key from Env
  var mapboxApiKey = process.env.mapbox_api_key;
  
  var drinks = [
    { name: 'Bloody Mary', drunkness: 3 },
    { name: 'Martini', drunkness: 5 },
    { name: 'Scotch', drunkness: 10 }
  ];
  var tagline = "Any code of your own that you haven't looked at for six or more months might as well have been written by someone else.";

  res.render('pages/index', {
    drinks: drinks,
    tagline: tagline,
    mapboxApiKey: mapboxApiKey
  });

});

// about page 
app.get('/about', function (req, res) {
  res.render('pages/about');
});

app.get('/tpx.gif', function (req, res, next) {

  console.log("Cookies: ", req.cookies)

  io.emit('visit', {
    ip: req.ip,
    ua: req.headers['user-agent']
  });

  emptygif.sendEmptyGif(req, res, {
    'Content-Type': 'image/gif',
    'Content-Length': emptygif.emptyGifBufferLength,
    'Cache-Control': 'public, max-age=0' // or specify expiry to make sure it will call everytime
  });
});

console.log(__dirname)

app.use(express.static(__dirname + '/public'));

server.listen(1337);
