const express = require('express');
const bodyParser = require('body-parser');
const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');
const Redis = require('ioredis');
const cors = require('cors');

const app = express();
const PORT = 5000;

app.use(cors());
app.use(bodyParser.json());

const mqttBrokerUrl = 'mqtt://localhost';
const mqttClient = mqtt.connect(mqttBrokerUrl);

const redisConfig = {
    host: 'redis-12675.c212.ap-south-1-1.ec2.cloud.redislabs.com',
    port: 12675,
    username: 'default',
    password: 'dssYpBnYQrl01GbCGVhVq2e4dYvUrKJB'
};

const redisClient = new Redis({
    host: redisConfig.host,
    port: redisConfig.port,
    username: redisConfig.username,
    password: redisConfig.password,
});

// const redisClient = new Redis('rediss://default:dssYpBnYQrl01GbCGVhVq2e4dYvUrKJB@redis-12675.c212.ap-south-1-1.ec2.cloud.redislabs.com');

redisClient.on('connect', () => {
    console.log('Connected to Redis');
});
redisClient.on('error', (err) => {
    console.error('Redis connection error:', err);
});

const mongoUrl = 'mongodb+srv://assignment_user:HCgEj5zv8Hxwa4xO@test-cluster.6f94f5o.mongodb.net/';
const mongoClient = new MongoClient(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true });
let mongoCollection;

mqttClient.on('connect', () => {
    mqttClient.subscribe('/add', (err) => {
        if (err) {
            console.error('MQTT Subscribe error:', err);
        } else {
            console.log('Connected to MQTT broker');
        }
    });
});

mqttClient.on('message', (topic, message) => {
    if (topic === '/add') {
        const task = message.toString();
        console.log(task)
        redisClient.get('FULLSTACK_TASK_SURYA_PRATAP', (err, reply) => {
            if (err) {
                console.error('Redis get error:', err);
                return;
            }
            let tasks = [];
            if (reply) {
                tasks = JSON.parse(reply);
            }
            tasks.push(task);
            redisClient.set('FULLSTACK_TASK_SURYA_PRATAP', JSON.stringify(tasks), (err) => {
                if (err) {
                    console.error('Redis set error:', err);
                    return;
                }
                if (tasks.length > 50) {
                    moveTasksToMongo(tasks);
                }
            });
        });
    }
});

async function connectToMongo() {
    try {
        await mongoClient.connect();
        const db = mongoClient.db('assignment');
        mongoCollection = db.collection('assignment_SURYA_PRATAP');
        console.log('Connected to MongoDB');
    } catch (err) {
        console.error('MongoDB connection error:', err);
    }
}

connectToMongo();

async function moveTasksToMongo(tasks) {
    try {
        for (const content of tasks) {
            await mongoCollection.insertOne({ content });
        }
        redisClient.del('FULLSTACK_TASK_SURYA_PRATAP');
        console.log('Tasks moved to MongoDB');
    } catch (err) {
        console.error('MongoDB insertion error:', err);
    }
}

app.get('/fetchAllTasks', (req, res) => {
    redisClient.get('FULLSTACK_TASK_SURYA_PRATAP', (err, reply) => {
        if (err) {
            console.error('Redis get error:', err);
            res.status(500).send('Internal Server Error');
            return;
        }
        let tasks = [];
        if (reply) {
            tasks = JSON.parse(reply);
        }
        res.json(tasks);
    });
});

app.post('/add', (req, res) => {
    const task = req.body.content;
    console.log(task)
    redisClient.get('FULLSTACK_TASK_SURYA_PRATAP', (err, reply) => {
        if (err) {
            console.error('Redis get error:', err);
            res.status(500).send('Internal Server Error');
            return;
        }
        let tasks = [];
        if (reply) {
            tasks = JSON.parse(reply);
        }
        tasks.push(task);
        redisClient.set('FULLSTACK_TASK_SURYA_PRATAP', JSON.stringify(tasks), (err) => {
            if (err) {
                console.error('Redis set error:', err);
                res.status(500).send('Internal Server Error');
                return;
            }
            res.status(200).send('Task added successfully');
            if (tasks.length > 50) {
                moveTasksToMongo(tasks);
            }
        });
    });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});