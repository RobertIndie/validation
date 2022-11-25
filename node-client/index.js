const Pulsar = require('sn-pulsar-client');

(
    async () => {
        const client = new Pulsar.Client({
            serviceUrl: 'pulsar://localhost:6650',
            operationTimeoutSeconds: 30,
        });

        const topic = 'persistent://public/default/reader-seek-timestamp';
        const producer = await client.createProducer({
            topic,
            sendTimeoutMs: 30000,
            batchingEnabled: false,
        });

        for (let i = 0; i < 10; i += 1) {
            const msg = `my-message-${i}`;
            console.log(msg);
            await producer.send({
                data: Buffer.from(msg),
            });
        }

        const reader = await client.createReader({
            topic,
            startMessageId: Pulsar.MessageId.latest(),
        });

        const currentTime = Date.now();
        console.log(currentTime);

        await reader.seekTimestamp(currentTime);

        console.log('End seek');

        console.log(reader.hasNext());


        await reader.seekTimestamp(currentTime - 100000);
        console.log('Seek to previous time');

        const msg = reader.readNext(1000);
        console.log((await msg).getMessageId().toString());
        console.log((await msg).getData().toString());

        await producer.close();
        await reader.close();
        await client.close();

    }
)();

