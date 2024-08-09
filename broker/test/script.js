import { Client, StatusOK } from 'k6/net/grpc';
import { check } from 'k6';
import { b64encode } from 'k6/encoding';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

export const options = {
  vus: 30000,
  duration: '30s',
  gracefulStop: '3s',
};

const client = new Client();
client.load(['../api/proto/'], 'broker.proto');

export default function() {
  client.connect("localhost:50051", { plaintext: true });

  const data = { subject: 'ali' ,
    body: b64encode(randomString(10, 'asdfghjklzxcvbnmqwertyuiop')),
    expirationSeconds: 50 };
  const response = client.invoke('/broker.Broker/Publish', data);

  check(response, {
    'status is OK': (r) => r && r.status === StatusOK,
  });

  client.close();
}