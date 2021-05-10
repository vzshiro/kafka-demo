const { Kafka } = require('kafkajs')
const express = require('express');
const app = express();
var http = require('http').createServer(app);
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const broker = process.env.BROKER || '192.168.183.132:9092'

app.use(express.static('public', { maxAge: 60 }))
const wsServer = new WebSocket.Server({ server: http }, () => console.log(`WS server is listening at ws://localhost:${WS_PORT}`));

const topics = ["default-topic", "topic-A", "topic-B", "topic-C"]
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: [broker],
  retry: {
    initialRetryTime: 1000,
    retries: 20
  }
})
const producer = kafka.producer()
async function initProducer() {
  await producer.connect()
}
async function produceMessage(data) {
  // await producer.connect()
  await producer.send({
    topic: data.topic,
    messages: [
      { value: data.message },
    ],
  })
  console.log("Sent a message")
  // await producer.disconnect()
}

const consumer = kafka.consumer({ groupId: 'test-group' })
async function initConsumer() {
  await consumer.connect()
  console.log("Consumer connected")
  var promises = topics.reduce((acc, topic) => {
    acc.push(consumer.subscribe({ topic: topic, fromBeginning: true }));
    return acc;
  }, [])
  await Promise.all(promises);
  console.log("Consumer subscribed to all topics")
  
  await consumer.run({
    eachMessage: processMessage,
  })
}

async function processMessage({ topic, partition, message }) {
  var currentTime = new Date();
  var data = {
    topic: topic,
    value: message.value.toString(),
    timestamp: new Date(+message.timestamp),
    processDelay: (currentTime.getTime() - new Date(+message.timestamp).getTime()) / 1000 + ' sec'
  }
  console.log(data);
  sendMessageToClients(data);
}

initProducer();
initConsumer();

let connectedClients = [];

function sendMessageToClients(data) {
  var i = 0;
  while(i != connectedClients.length) {
    if (connectedClients[i].readyState === connectedClients[i].OPEN) { // check if it is still connected
      connectedClients[i].send(JSON.stringify(data)); // send
    } else {
      connectedClients.splice(i, 1);
      i--;
    }
    i++;
  }
}
wsServer.on('connection', (ws, req) => {
  console.log('Connected', new Date());
  connectedClients.push(ws);
  // Provide list of topics to html client
  ws.send(JSON.stringify({ topics: topics }))
  ws.on('message', data => {
    var data = JSON.parse(data)
    produceMessage(data);
  });
});

app.get('/', (req, res) => res.sendFile(path.resolve(__dirname, './index.html')));
http.listen(PORT, () => {
  console.log(`listening on http://localhost:${PORT}`);
});