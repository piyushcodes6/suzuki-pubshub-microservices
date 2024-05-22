const express = require('express');
const bodyParser = require('body-parser');
const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');
const Redis = require('ioredis');
const redis = new Redis({ host: process.env.REDIS_HOST });

const app = express();
app.use(bodyParser.json());

mongoose.connect(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });

const DataSchema = new mongoose.Schema({
  id: String,
  user: String,
  class: String,
  age: Number,
  email: String,
  inserted_at: Date
});

const DataModel = mongoose.model('Data', DataSchema);

app.post('/receiver', async (req, res) => {
  const { user, class: className, age, email } = req.body;

  if (!user || !className || !age || !email) {
    return res.status(400).send('Invalid data');
  }

  const newData = new DataModel({
    id: uuidv4(),
    user,
    class: className,
    age,
    email,
    inserted_at: new Date()
  });

  await newData.save();

  // Publishing event to Redis
  redis.publish('data_inserted', JSON.stringify(newData));

  res.status(201).send(newData);
});

app.listen(3000, () => {
  console.log('Receiver service running on port 3000');
});
