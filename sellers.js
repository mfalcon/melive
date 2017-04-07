var express = require('express');
var app = express();
app.use(express.static(__dirname + '/public'));


var http = require('http').Server(app);


var io = require('socket.io')(http);
var sredis = require('socket.io-redis');
io.adapter(sredis({ host: 'localhost', port: 6379 }));

var redis = require('redis');

redisSubscriber = redis.createClient(6379, 'localhost', {});


redisSubscriber.on('message', function(channel, message) {
  io.emit(channel, message);
});


app.get('/', function(req, res){
  res.sendfile('sellers_d3js.html');
});

app.get('/sellers/:seller_id', function(req, res){
  var seller_id = req.params.seller_id;
  redisSubscriber.subscribe('sellers-'.concat(seller_id)); //FIXME: proper syntax
  console.log(seller_id);

  res.render( 'seller.ejs', { seller:seller_id } );
});


http.listen(3000, '0.0.0.0', function(){
  console.log('listening on *:3000');
});

