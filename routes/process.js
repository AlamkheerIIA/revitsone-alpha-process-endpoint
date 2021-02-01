var express = require('express');
var router = express.Router();
var pro = require('./dataProcess/processor');
/* GET users listing. */
router.post('/', function(req, res, next) {

  if(req.body && Object.keys(req.body).length){
    pro.firehoseConsumer(req.body);
    res.status(200).send({ 'error': false });

  }else{
    console.log('Required Fields are missing.');
    res.status(400).send({ 'error': true });
  }
});

module.exports = router;
