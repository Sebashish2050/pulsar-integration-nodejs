const Pulsar = require('pulsar-client');

(async () => {
  let client;
  let consumer;
  try {
    // Create a client
  client = new Pulsar.Client({
    serviceUrl: 'pulsar://localhost:6650',
  });

  // Create a consumer
  consumer = await client.subscribe({
    topic: 'my-topic',
    subscription: 'my-subscription',
    subscriptionType: 'Failover',
  });

  // Receive messages
  while(true) {
    const msg = await consumer.receive();
    console.log(msg.getData().toString());
    const messageId = msg.getMessageId();
    consumer.acknowledgeId(messageId);
  }
  } catch (error) {
    console.log(error);
  } finally {
    await consumer.close();
    await client.close();
  }
})();
