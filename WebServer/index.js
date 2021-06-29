const { CompressionTypes, Kafka } = require('kafkajs')
const express = require('express');
const app = express();
var http = require('http').createServer(app);
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const brokers = process.env.BROKERS || 'tepdiprtl001.seagate.com:9092,tepdiprtl002.seagate.com:9092,tepdiprtl003.seagate.com:9092'
// const partitions = brokers.split(",").length

app.use(express.static('public', { maxAge: 60 }))
const wsServer = new WebSocket.Server({ server: http }, () => console.log(`WS server is listening at ws://localhost:${WS_PORT}`));

const topics = { "hs-topic-1": 3, "hs-topic-2": 1, "hs-topic-3": 2, "hs-topic-4": 3 }
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
async function produceMessage(data, ws, cacheMsg) {
  switch (data.trigger) {
    case "stopRepeat":
      ws.repeat = false;
      return;
    case "stopCompress":
      ws.compress = false;
      return;
    case "compress":
      ws.compress = true;
      return;
    case "multiply":
      ws.multiply = data.value;
      return;
  }
  
  var message = { value: data.message };
  var messages = cacheMsg || [message]
  if (ws.multiply) {
    if (!cacheMsg || cacheMsg.length != ws.multiply) {
      messages = new Array(ws.multiply);
      for (let i=0; i<ws.multiply; ++i) messages[i] = message;
    }
  }
  await producer.send({
    topic: data.topic,
    compression: ws.compress ? CompressionTypes.GZIP : CompressionTypes.None,
    messages: messages
  })
  ws.msgSent += messages.length;
  ws.msgPerSec += messages.length;

  if (ws.repeat) {
    data.key++;
    produceMessage(data, ws, messages);
  }
}

// const consumer = kafka.consumer({ groupId: 'test-group' })
async function createTopics() {
  console.log(generateTopicConfig())
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
}
async function createConsumer(ws, fromBeginning, groupId) {
  var consumer = kafka.consumer({ groupId: groupId || 'test-group' + ws.id, maxWaitTimeInMs: 50 })
  await consumer.connect();
  console.log("New consumer connected")
  
  var promises = Object.keys(topics).reduce((acc, topic) => {
    acc.push(consumer.subscribe({ topic: topic, fromBeginning: fromBeginning }));
    return acc;
  }, [])
  await Promise.all(promises);
  console.log("Consumer subscribed to all topics")
  
  consumer.run({
    partitionsConsumedConcurrently: 3,
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
        ws.consumePerSec++;
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
  return Object.keys(topics).reduce((acc, topic) => {
    acc.push({
      topic: topic,
      numPartitions: topics[topic],
    })
    return acc;
  }, [])
}

function processedMessage(ws) {
  if (ws.readyState === ws.OPEN) {
    var data = {
      metrics: true,
      msgSent: ws.msgSent,
      msgPerSec: ws.msgPerSec,
      msgProcessed: ws.msgProcessed,
      consumePerSec: ws.consumePerSec,
      // lag: ws.msgSent - ws.msgProcessed, // Inaccurate
      lastMessage: ws.lastMessage
    }
    ws.send(JSON.stringify(data));
    return true;
  } else {
    return false;
  }
}

function publishMetrics(ws) {
  let interval = setInterval(() => {
    if (!processedMessage(ws)) {
      clearInterval(interval);
    }
    ws.msgPerSec = 0;
    ws.consumePerSec = 0;
  }, 1000)
}

function resetMetrics(ws) {
  ws.msgSent = 0;
  ws.msgPerSec = 0;
  ws.msgProcessed = 0;
  ws.consumePerSec = 0;
  ws.lastMessage = {};
}

function recreateTopics() {
  admin.deleteTopics({
    topics: Object.keys(topics),
    timeout: 5000,
  }).then((res) => {
    console.log(res)
    return true;
  }).catch((err) => {
    console.log("Error recreating topic", err)
    return false;
  }).finally(() => {
    createTopics();
  })
}

function getTopicsMetadata(ws) {
  admin.fetchTopicMetadata({ topics: Object.keys(topics) }).then((data) => {
    ws.send(JSON.stringify({ metadata: true, data: data }));
  })
}

function initWebSocket(ws) {
  ws.id = wsServer.getUniqueID();
  resetMetrics(ws);
}

initProducer();
createTopics();

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
  ws.send(JSON.stringify({ topics: Object.keys(topics) }))
  publishMetrics(ws);
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
      case "recreateTopics":
        recreateTopics(ws);
        break;
      case "getTopicsMetadata":
        getTopicsMetadata(ws);
        break;
      default:
        if (data.repeatMsg) {
          ws.repeat = true;
          data.repeatMsg = false;
        }
        if (data.compress) {
          ws.compress = true;
          data.compress = false;
        }
        ws.multiply = data.multiply;
        produceMessage(data, ws);
        break;
    }
  });
});

app.get('/', (req, res) => res.sendFile(path.resolve(__dirname, './index.html')));
http.listen(PORT, () => {
  console.log(`listening on http://localhost:${PORT}`);
});