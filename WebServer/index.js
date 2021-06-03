const { Kafka } = require('kafkajs')
const express = require('express');
const app = express();
var http = require('http').createServer(app);
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const brokers = process.env.BROKER || '192.168.183.132:9092,192.168.183.132:19092,192.168.183.132:39092'
// const brokers = '192.168.1.12:9092,192.168.1.12:19092,192.168.1.12:39092'
const partitions = brokers.split(",").length

app.use(express.static('public', { maxAge: 60 }))
const wsServer = new WebSocket.Server({ server: http }, () => console.log(`WS server is listening at ws://localhost:${WS_PORT}`));

const topics = ["test-topic", "default-topic", "topic-A", "topic-B", "topic-C"]
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: brokers.split(','),
  retry: {
    initialRetryTime: 1000,
    retries: 20
  }
})
const admin = kafka.admin()
const producer = kafka.producer()
async function initProducer() {
  await producer.connect()
}
async function produceMessage(data, ws) {
  // await producer.connect()
  var messages = [{ value: data.message, partition: data.key%partitions }]
  if (data.multiply) {
    messages = Array.from({length:1000}).map(x => messages[0])
  }
  ws.msgSent += messages.length;
  await producer.send({
    topic: data.topic,
    messages: messages
  })
  // console.log("Message sent")
  // await producer.disconnect()
}

const consumer = kafka.consumer({ groupId: 'test-group' })
async function initConsumer() {
  await admin.createTopics({
    validateOnly: false,
    waitForLeaders: true,
    timeout: 5000,
    topics: generateTopicConfig(),
  }).catch(err => {
    console.log("Unable to create topics", err)
  })
  // await admin.listTopics().then(data => {
  //   console.log("Topics", data)
  // })
  return;
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
async function createConsumer(ws, fromBeginning, groupId) {
  var consumer = kafka.consumer({ groupId: groupId || 'test-group' + ws.id, maxWaitTimeInMs: 50 })
  await consumer.connect();
  console.log("New consumer connected")
  
  var promises = topics.reduce((acc, topic) => {
    acc.push(consumer.subscribe({ topic: topic, fromBeginning: fromBeginning }));
    return acc;
  }, [])
  await Promise.all(promises);
  console.log("Consumer subscribed to all topics")
  
  await consumer.run({
    eachMessage: ({topic, partition, message}) => {
      var currentTime = new Date();
      var data = {
        topic: topic,
        partition: partition,
        consumer: "Custom",
        value: message.value.toString(),
        timestamp: new Date(+message.timestamp),
        processDelay: (currentTime.getTime() - new Date(+message.timestamp).getTime()) / 1000 + ' sec'
      }
      // console.log(data);
      // if (ws.readyState === ws.OPEN) { // check if it is still connected
      //   ws.send(JSON.stringify(data)); // send
      // }
      // Remove consumer if not connected
      if (ws.readyState === ws.OPEN) {
        ws.msgProcessed++;
        ws.lastMessage = data;
      } else {
        consumer.disconnect();
        console.log("Consumer has been disconnected")
      }
      // console.log(message.value.toString())
    },
  })
}

wsServer.getUniqueID = function () {
  function s4() {
      return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
  }
  return s4() + s4() + '-' + s4();
};

function generateTopicConfig() {
  return topics.reduce((acc, topic) => {
    acc.push({
      topic: topic,
      numPartitions: partitions,
    })
    return acc;
  }, [])
}

function processMessage({ topic, partition, message }) {
  var currentTime = new Date();
  var data = {
    topic: topic,
    partition: partition,
    consumer: "Default",
    value: message.value.toString(),
    timestamp: new Date(+message.timestamp),
    processDelay: (currentTime.getTime() - new Date(+message.timestamp).getTime()) / 1000 + ' sec'
  }
  // console.log(data);
  sendMessageToClients(data);
}

function processedMessage(ws) {
  if (ws.readyState === ws.OPEN) {
    var data = {
      metrics: true,
      msgSent: ws.msgSent,
      msgProcessed: ws.msgProcessed,
      lag: ws.msgSent - ws.msgProcessed,
      lastMessage: ws.lastMessage
    }
    ws.send(JSON.stringify(data));
  }
}

function resetMetrics(ws) {
  ws.msgSent = 0;
  ws.msgProcessed = 0;
  ws.lastMessage = {};
}

function initWebSocket(ws) {
  ws.id = wsServer.getUniqueID();
  resetMetrics(ws);
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
  initWebSocket(ws);
  connectedClients.push(ws);
  // Provide list of topics to html client
  ws.send(JSON.stringify({ topics: topics }))
  ws.on('message', data => {
    var data = JSON.parse(data)
    switch (data.action) {
      case "consumer":
        createConsumer(ws, data.fromStart, data.groupId);
        break;
      case "processedMsg":
        processedMessage(ws);
        break;
      case "resetMetrics":
        resetMetrics(ws);
        break;
      default:
        produceMessage(data, ws);
        break;
    }
  });
});

app.get('/', (req, res) => res.sendFile(path.resolve(__dirname, './index.html')));
http.listen(PORT, () => {
  console.log(`listening on http://localhost:${PORT}`);
});