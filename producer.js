const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar://localhost:6650',
  });

  const producer = await client.createProducer({
    topic: 'my-topic',
  });

  // Send messages
  for (let i = 0; i < 2; i += 1) {
    const msg = `my-message-${i}`;
    producer.send({
      data: Buffer.from(msg),
    });
    console.log(`Sent message: ${msg}`);
  }

  await producer.flush();
  await producer.close();
  await client.close();
})();