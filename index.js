const express = require('express');
const AWS = require('aws-sdk');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());




const sns = new AWS.SNS();
const sqs = new AWS.SQS();


app.post('/create-topic-subscriber', async (req, res) => {
    let TopicArn = await createTopic(req.body.topicName);
    let QueueArn = await createQueue(req.body.queueName);

    console.log("TopicArn----->", TopicArn);
    console.log("QueueArn----->", QueueArn);
    return res.status(200).json({TopicArn, QueueArn})
});

app.post('/publish-message', async (req, res) => {
    const topicName = req.body.topicName;
    const message = JSON.stringify(req.body.message);

    if (!topicName || !message) {
      return res.status(400).json({ message: 'Topic name and message are required' });
    }

    const topicArn = await createTopic(topicName);
    const queueArn = await createQueue(req.body.queueName);
    await subscribeQueueToTopic(queueArn, topicArn);
    await publishMessageToTopic(topicArn, message);
    return res.status(200).json({ message: 'Message published to topic' });
})


async function createTopic(topicName) {
    if (!topicName) {
      throw new Error('Topic name is required');
    }
  
    const params = {
      Name: topicName,
    };
  
    const data = await sns.createTopic(params).promise();
    return data.TopicArn;
}

async function createQueue(queueName) {
    if (!queueName) {
      throw new Error('Queue name is required');
    }
  
    const params = {
      QueueName: queueName,
    };
  
    const data = await sqs.createQueue(params).promise();
    console.log("data", data);
    return await getQueueArn(data.QueueUrl);
}

async function getQueueArn(queueUrl) {
    if (!queueUrl) {
      throw new Error('Queue URL is required');
    }
  
    const params = {
      QueueUrl: queueUrl,
      AttributeNames: ['QueueArn'],
    };
  
    const data = await sqs.getQueueAttributes(params).promise();
    return data.Attributes.QueueArn;
}

async function subscribeQueueToTopic(queueUrl, topicArn) {
    return new Promise((resolve, reject) => {
        const params = {
          Protocol: 'sqs',
          TopicArn: topicArn,
          Endpoint: queueUrl,
        };
    
        sns.subscribe(params, (err, data) => {
          if (err) {
            reject(err);
          } else {
            resolve(data);
          }
        });
      });
}

async function publishMessageToTopic(topicArn, message) {
    const params = {
      TopicArn: topicArn,
      Message: message,
    };
  
    await sns.publish(params, (err, data) => {
        if(err){
            console.log("err------>", err);
        } else {
          console.log("data-------->113", data);
        }
    }).promise();
}

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});