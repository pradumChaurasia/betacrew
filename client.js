const net = require('net');
const fs = require('fs');

const SERVER_HOST = 'localhost';
const SERVER_PORT = 3000;
const CALL_TYPE_STREAM_ALL_PACKETS = 1;
const CALL_TYPE_RESEND_PACKET = 2;

let packets = [];
let receivedSequences = new Set();

// Establish a TCP connection to the server
const client = new net.Socket();

client.connect(SERVER_PORT, SERVER_HOST, () => {
  console.log('Connected to BetaCrew server.');

  // Send a request to stream all packets
  const buffer = Buffer.alloc(2);
  buffer.writeInt8(CALL_TYPE_STREAM_ALL_PACKETS, 0);
  buffer.writeInt8(0, 1); // resendSeq is not used for stream all packets call

  client.write(buffer);
});

client.on('data', (data) => {
  // Read packets from the server
  while (data.length >= 17) {
    const packet = {
      symbol: data.slice(0, 4).toString('ascii'),
      buysellindicator: data.slice(4, 5).toString('ascii'),
      quantity: data.readInt32BE(5),
      price: data.readInt32BE(9),
      packetSequence: data.readInt32BE(13)
    };

    packets.push(packet);
    receivedSequences.add(packet.packetSequence);

    data = data.slice(17);
  }
});

client.on('close', () => {
  console.log('Connection closed.');

  // Find missing sequences
  const missingSequences = [];
  const maxSequence = Math.max(...Array.from(receivedSequences));
  for (let i = 1; i <= maxSequence; i++) {
    if (!receivedSequences.has(i)) {
      missingSequences.push(i);
    }
  }

  // Request missing packets
  const requestMissingPackets = (seqIndex) => {
    if (seqIndex >= missingSequences.length) {
      // All missing packets have been requested, save the JSON file
      packets.sort((a, b) => a.packetSequence - b.packetSequence);
      fs.writeFileSync('packets.json', JSON.stringify(packets, null, 2));
      console.log('JSON file created.');
      return;
    }

    const buffer = Buffer.alloc(2);
    buffer.writeInt8(CALL_TYPE_RESEND_PACKET, 0);
    buffer.writeInt8(missingSequences[seqIndex], 1);

    const resendClient = new net.Socket();
    resendClient.connect(SERVER_PORT, SERVER_HOST, () => {
      resendClient.write(buffer);
    });

    resendClient.on('data', (data) => {
      const packet = {
        symbol: data.slice(0, 4).toString('ascii'),
        buysellindicator: data.slice(4, 5).toString('ascii'),
        quantity: data.readInt32BE(5),
        price: data.readInt32BE(9),
        packetSequence: data.readInt32BE(13)
      };

      packets.push(packet);
      receivedSequences.add(packet.packetSequence);

      resendClient.destroy();
    });

    resendClient.on('close', () => {
      requestMissingPackets(seqIndex + 1);
    });
  };

  if (missingSequences.length > 0) {
    requestMissingPackets(0);
  } else {
    packets.sort((a, b) => a.packetSequence - b.packetSequence);
    fs.writeFileSync('packets.json', JSON.stringify(packets, null, 2));
    console.log('JSON file created.');
  }
});

client.on('error', (err) => {
  console.error('Error: ', err);
});
