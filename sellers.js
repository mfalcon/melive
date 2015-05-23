var express = require('express');
var app = express();
app.use(express.static('js'));


var http = require('http').Server(app);
var io = require('socket.io')(http);
var redis = require('redis');

var redisSubscriber = redis.createClient();

var socketIORedis = require('socket.io-redis');
io.adapter(socketIORedis({ host: 'localhost', port: 6379 }));

//redisSubscriber.subscribe('sellers');
//redisSubscriber.subscribe('sellers:80183917');

redisSubscriber.on('message', function(channel, message) {
  io.emit(channel, message);
});


app.get('/', function(req, res){
  res.sendfile('sellers_d3js.html');
});


app.get('/sellers/:seller_id', function(req, res){
  var seller_id = req.params.seller_id;
  redisSubscriber.subscribe('sellers:'.concat(seller_id)); //FIXME: proper syntax
  console.log(seller_id);

  res.render( 'seller.ejs', { seller:seller_id } );
});

io.on('connection', function(socket){
  console.log('a user connected');
  socket.on('disconnect', function(){
    console.log('user disconnected');
  });
  socket.on('chat message', function(msg){
    io.emit('chat message', msg);
  });
});

http.listen(3000, function(){
  console.log('listening on *:3000');
});

