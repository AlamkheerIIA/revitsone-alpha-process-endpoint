var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.json({'status':'running'}).statusCode(200);
});

module.exports = router;
