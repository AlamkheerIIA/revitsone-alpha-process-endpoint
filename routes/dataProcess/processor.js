
'use strict';
var async = require("async");
var aws = require('aws-sdk');
var moment = require("moment");
var elasticsearch = require('elasticsearch');
var unirest = require('unirest');
aws.config.update({
    accessKeyId: process.env.AWS_APP_ACCESSKEY,
    secretAccessKey: process.env.AWS_APP_SECRETKEY,
    region: process.env.AWS_APP_REGION,
});
var redis   = require("redis");

var s3bucket = new aws.S3({ params: { Bucket: process.env.S3_BACKUP_BUCKET } });
var kinesis = new aws.Kinesis();
var timezones = {},unicDetails  = {},out_orginal_data=[];
var clientRedis,elastic_search_client,redisconnectionflag=0,elasticsearchconnectionflag=0,unicParams=[],bulkSecondaryVDetailsInsert=[];
let domains = JSON.parse(process.env.DOMAINS);
// console.log('Domains',domains);
let connections = {};
let domaindata  = {};

for(let j in domains) {
    domaindata[j] = [];
}
module.exports.firehoseConsumer = async (reqEvent) => {
    // console.log(reqEvent);
    var event = JSON.parse(reqEvent);
    // console.log('Event \n',event,'\n');
    function redisConnection() {
        return new Promise(resolve => {
            var flag = 0;
            const REDIS_URL = process.env.REDIS_HOST;
            clientRedis = redis.createClient(REDIS_URL); 
            clientRedis.on('connect', () => {
                // console.log('------------SUCCESSFULLY CONNECTED TO REDIS-----------', process.env.REDIS_HOST);
                flag = 1;
                redisconnectionflag=1;
                resolve(true);
            });
            clientRedis.on('error', err => {
                console.log(`Error: ${err}`);
                flag = 1;
                redisconnectionflag=0;
                resolve(false);
            });
            setTimeout(function(){
                if(flag == 0) {
                    console.log('in redis conenction fallback');
                    redisconnectionflag=0;
                    resolve(false);
                }
            },5000);
        });
    }
    function connecttoElasticsearch(domain='') {
        return new Promise(resolve => {
            if((elasticsearchconnectionflag == 1 && domain=='') || (domain!='' && connections[domain]  && connections[domain] !='')) {
                resolve();
            } else {
                var flag = 0;
                if(domain && domain!='') {
                    // console.log('trying to connect domain elastic search ',domain);
                    connections[domain] = new elasticsearch.Client({
                        host: domains[domain]['host'],
                        httpAuth: domains[domain]['httpAuth']                          
                    });
                    connections[domain].ping({
                        requestTimeout: 10000
                    }, function (error) {
                        if (error) {
                            // console.log("---------------error in elastic search domain connection-------------------",domain,error);
                            flag=1;
                            resolve();
                        } else {
                            // console.log("--------------------elastic search domain connected------------------------",domain);
                            flag=1;
                            resolve();
                        }
                    });
                } else {
                    elastic_search_client = new elasticsearch.Client({
                        host: domains['default']['host'],
                        httpAuth: domains['default']['httpAuth']                          
                    });
                    elastic_search_client.ping({
                        requestTimeout: 10000
                    }, function (error) {
                        if (error) {
                            console.log("---------------error in elastic search connection-------------------",error);
                            elasticsearchconnectionflag = 0;
                            flag=1;
                            resolve();
                        } else {
                            console.log("--------------------elastic search connected------------------------");
                            elasticsearchconnectionflag = 1;
                            flag=1;
                            resolve();
                        }
                    });
                }
               
                setTimeout(function(){
                    if(flag == 0) {
                        console.log('in elastic search conenction fallback');
                        if(domain && domain!='') {
                            connections[domain] = '';
                        } else {
                            elasticsearchconnectionflag=0;
                        }
                        resolve();
                    }
                },10000);
            }
        });
    }
    function setRedisKey(key,value) {
        return new Promise(resolve => {
            if(redisconnectionflag == 0) {
                resolve();
            } else {
                var flag = 0;
                clientRedis.set(key,value,(err, result) => {
                    flag = 1;
                    resolve();
                });
                setTimeout(function(){
                    if(flag == 0) {
                        resolve();
                    }
                },5000);
            }
        });
    }
    function getRedisKey(key) {
        return new Promise(resolve => {
            if(redisconnectionflag == 0) {
                resolve({});
            } else {
                var flag = 0;
                clientRedis.get(key,(err, result) => {
                    flag = 1;
                    if(result && result != null) {
                        resolve(JSON.parse(result));
                    } else {
                        resolve({});
                    }
                    
                });
                setTimeout(function(){
                    if(flag == 0) {
                        resolve({});
                    }
                },5000);
            }
        });
    }

    function parseAlphadevice(packet_data,result) {
        if(packet_data.bit_status) {
            if(result.D0 && result.D0 !='') {
                packet_data[result.D0] = packet_data.bit_status[4];
            }
            if(result.D1 && result.D1 !='') {
                packet_data[result.D1] = packet_data.bit_status[3];
            }
            if(result.D2 && result.D2 !='') {
                packet_data[result.D2] = packet_data.bit_status[2];
            }
        }
        if(packet_data['Data 1']) {
            let infodetails = packet_data['Data 1'].split('#');
            if(result.Data1info1 && result.Data1info1!='' && infodetails[0]) {
                packet_data[result.Data1info1] = infodetails[0];

            }
            if(result.Data1info2 && result.Data1info2!='' && infodetails[1]) {
                packet_data[result.Data1info2] = infodetails[1];

            }
            if(result.Data1info3 && result.Data1info3!=''  && infodetails[2]) {
                packet_data[result.Data1info3] = infodetails[2];
            }
            if(result.Data2info1 && result.Data2info1!=''  && infodetails[2]) {
                packet_data[result.Data2info1] = packet_data['Data 2'];
            }
        }
        return packet_data;
    }
    function fallbackelasticsearch(device_id) {
        return new Promise(resolve => {
            async function asyncfallback() {
                let query = { 
                    'query': { 
                        'bool': { 
                            'must': [
                                        { 'term': { 'device_id.keyword': device_id }},
                                        { 'term': { 'itenary_status': 'progress' }},
                                    ], 
                        }
                    }
                };
                let details = {};
                await getDatafromelasticsearch('assignments','assignments',query,1).then((assignment_details) => {
                    if(assignment_details != null && assignment_details != undefined && assignment_details != '' && assignment_details.hits.hits.length > 0){
                            details =  {
                                            vehicle_number:assignment_details.hits.hits[0].vehicle_number,
                                            vehicle_name:assignment_details.hits.hits[0].vehicle_name,
                                            vehicle_id:assignment_details.hits.hits[0].vehicle_id,
                                            device_id:assignment_details.hits.hits[0].device_id,
                                            vehicle_group:assignment_details.hits.hits[0].vehicle_group,
                                            company_id:assignment_details.hits.hits[0].company_id,
                                            department_id:assignment_details.hits.hits[0].department_id,
                                            group_id:assignment_details.hits.hits[0].group_id,
                                            user_id:assignment_details.hits.hits[0].user_id,
                                            assignment_id:assignment_details.hits.hits[0].id,
                                            route_id:assignment_details.hits.hits[0].route_id,
                                            vehicle_make:assignment_details.hits.hits[0].vehicle_make,
                                            vehicle_model:assignment_details.hits.hits[0].vehicle_model,
                                            vehicle_year:assignment_details.hits.hits[0].vehicle_year,
                                            vehicle_trim:assignment_details.hits.hits[0].vehicle_trim,
                                            vehicle_type:assignment_details.hits.hits[0].vehicle_type,
                                            driver_name:assignment_details.hits.hits[0].driver_name,
                                            driver_id:assignment_details.hits.hits[0].driver_id,
                                            D0:assignment_details.hits.hits[0].D0,
                                            D1:assignment_details.hits.hits[0].D1,
                                            D2:assignment_details.hits.hits[0].D2,
                                            Data1info1:assignment_details.hits.hits[0].Data1info1,
                                            Data1info2:assignment_details.hits.hits[0].Data1info2,
                                            Data1info3:assignment_details.hits.hits[0].Data1info3,
                                            Data2info1:assignment_details.hits.hits[0].Data2info1,
                                            vehicle_fuel_tank1_capacity:assignment_details.hits.hits[0].vehicle_fuel_tank1_capacity,
                                            calibration_type:assignment_details.hits.hits[0].calibration_type,
                                            device_voltage_type:assignment_details.hits.hits[0].device_voltage_type,
                                            calibrated_values:assignment_details.hits.hits[0].calibrated_values,
                                            from_assignment:1,
                                            device_type:assignment_details.hits.hits[0].device_type

                                        };
                    }
                }).catch((err) => {
                    console.log('error occured',err);
                });
                if(!details.device_id) {
                    let vehicle_query = {
                        query: {
                            bool: {
                                should: [
                                    {
                                        match: {
                                            device_id: device_id
                                        }
                                    },
                                    {
                                        term: {
                                            is_delete: 0
                                        }
                                    }
                                ]
                            }
                        }
                    };
                    // console.log('----checking for vehcile information-----');
                    await getDatafromelasticsearch('vehicle_infomation', 'vehicle_infomation', vehicle_query, 1).then((vehicle_details) => {
                        // console.log(vehicle_details);
                        if(vehicle_details != null && vehicle_details != undefined && vehicle_details != '' && vehicle_details.hits.hits.length > 0){
                                details =   {
                                                vehicle_number:vehicle_details.hits.hits[0].vehicle_number,
                                                vehicle_name:vehicle_details.hits.hits[0].vehicle_name,
                                                vehicle_id:vehicle_details.hits.hits[0].vehicle_id,
                                                device_id:vehicle_details.hits.hits[0].device_id,
                                                vehicle_group:vehicle_details.hits.hits[0].vehicle_group,
                                                company_id:vehicle_details.hits.hits[0].company_id,
                                                department_id:vehicle_details.hits.hits[0].department_id,
                                                group_id:vehicle_details.hits.hits[0].group_id,
                                                user_id:vehicle_details.hits.hits[0].user_id,
                                                assignment_id:0,
                                                route_id:0,
                                                vehicle_make:vehicle_details.hits.hits[0].vehicle_make,
                                                vehicle_model:vehicle_details.hits.hits[0].vehicle_model,
                                                vehicle_year:vehicle_details.hits.hits[0].vehicle_year,
                                                vehicle_trim:vehicle_details.hits.hits[0].vehicle_trim,
                                                vehicle_type:vehicle_details.hits.hits[0].vehicle_type,
                                                driver_name:'',
                                                driver_id:'',
                                                D0:vehicle_details.hits.hits[0].D0,
                                                D1:vehicle_details.hits.hits[0].D1,
                                                D2:vehicle_details.hits.hits[0].D2,
                                                Data1info1:vehicle_details.hits.hits[0].Data1info1,
                                                Data1info2:vehicle_details.hits.hits[0].Data1info2,
                                                Data1info3:vehicle_details.hits.hits[0].Data1info3,
                                                Data2info1:vehicle_details.hits.hits[0].Data2info1,
                                                vehicle_fuel_tank1_capacity:vehicle_details.hits.hits[0].vehicle_fuel_tank1_capacity,
                                                calibration_type:vehicle_details.hits.hits[0].calibration_type,
                                                device_voltage_type:vehicle_details.hits.hits[0].device_voltage_type,
                                                calibrated_values:vehicle_details.hits.hits[0].calibrated_values,
                                                from_vehicle:1,
                                                device_type:vehicle_details.hits.hits[0].device_type

                                            };
                        }
                    }).catch(function (error) {
                        // console.log('------Didnt Get Vehicle Data From Elasticsearch----', error);
                    });
                }
                await setRedisKey('active_route_' + device_id,JSON.stringify(details)).then((res) => {
                   // console.log('----updated to redis for fallback---');
                }).catch(function (error) {
                   // console.log('----fallback redis error----', error);
                });
                resolve(details);
            }
            asyncfallback();
        });
    }
    function getDeviceTimezone(company_id) {
        return new Promise(resolve => {
            let flag = 0;
            if(!company_id || company_id=='') {
                flag = 1;
                resolve(0);
            }
            async function getTime() {
                if(timezones[company_id] && timezones[company_id]!='') {
                    flag = 1;
                    resolve(timezones[company_id]);
                }
                if(flag == 0) {
                    await getRedisKey('company_' + company_id).then((company_details) => {
                        if(company_details && company_details!=null) {
                            if(company_details && company_details.time_zone) {
                                let time_zone = company_details.time_zone;
                                if(time_zone && time_zone!='' && time_zone!=null) {
                                    time_zone = parseFloat(time_zone);
                                } else {
                                    time_zone = 0;
                                }
                                timezones[company_id] = time_zone;
                                resolve(time_zone);
                                flag=1;
                            } 
                        }
                    }).catch((err) => {
                        console.log('error occured---company_' + company_id, err);
                    });
                    if(flag == 0) {
                        let directquery = {
                            "_source": ["time_zone"],
                            "query": {
                            "bool": {
                                "must": [
                                
                                {
                                    "term": {
                                        "id": company_id
                                    }
                                }
                                
                                ]
                            }
                            }
                        };
                        await getDatafromelasticsearch('company', 'company', directquery, 1).then((address) => {
                           // console.log('------------Getting company data from Elastic Search direct query----------', address.hits.total);
                            if (address != null && address != undefined && address != '' && address.hits.hits.length > 0) {
                                let time_zone = address.hits.hits[0]._source.time_zone;
                                if(time_zone && time_zone!='' && time_zone!=null) {
                                    time_zone = parseFloat(time_zone);
                                } else {
                                    time_zone = 0;
                                }
                                timezones[company_id] = time_zone;
                                resolve(time_zone);
                            } else {
                                resolve(0);
                            }
                        }).catch((err) => {
                            resolve(0);
                        });
                    }
                }
            }
            if(flag==0) {
                getTime();
            }
                
        });
    }
    // function parselatLng(lat,long) {
    //     if(!lat || !long) {
    //         return {};
    //     }
    //     let newlat,newlong;
    //     newlat = parseFloat(lat.substr(0, 2)) + (parseFloat(lat.substr(2, 7)/60))
    //     if(lat.endsWith("S")) {
    //         newlat*=-1;
    //     }
    //     newlong = parseFloat(long.substr(0, 3)) + (parseFloat(long.substr(3, 7)/60))
    //     if(long.endsWith("W")) {
    //         newlong*=-1;
    //     }
    //     return {lat:newlat,long:newlong};
    // }
    function asyncRecordGet() {
        return new Promise(resolve => {
            if (event && event.records) {
                async.eachSeries(event.records, function (record, callback2) {
                    async function Iterator() {
                        const payload           = JSON.parse(new Buffer.from(record.data, 'base64').toString('utf-8'));
                        var packet_data         = payload['packet'];
                        var output_temp_data    = {};
                        var out_packet_data     = {};
                        // packet_data.deviceid    = '15A0005135'; 
                        packet_data.location_name = '';
                        
                        let addtnl_details = {};                 
                        await getRedisKey('active_route_' + packet_data.deviceid).then((result) => {
                            addtnl_details = result;
                            addtnl_details.from_cache = 1;
                        }).catch((err) => {
                            console.log('error occured',err);
                        });
                        if(!addtnl_details.device_id) {
                           // console.log('Didnt get  data from redis  for ' + packet_data.deviceid);
                            await fallbackelasticsearch(packet_data.deviceid).then((result) => {
                                addtnl_details = result;
                            }).catch((err) => {
                                console.log('error occured',err);
                            });
                        }
                        if(addtnl_details.device_type == "Alpha Tmp"){
                            packet_data.device_type = "Alpha Tmp";
                            packet_data.speed = packet_data.sog;
                            if (packet_data.speed == 0 && packet_data.vehicle_status == 1) {
                                packet_data.active_status = 1;
                            }
                            if (packet_data.speed != 0 && packet_data.vehicle_status == 1) {
                                packet_data.active_status = 3;
                            }
                        }
                        if (addtnl_details && addtnl_details.device_id) {
                            if (addtnl_details.route_id) {
                                packet_data.route_id = addtnl_details.route_id;
                            }
                            if (addtnl_details.assignment_id) {
                                packet_data.assignment_id = addtnl_details.assignment_id;
                            }
                            if(!packet_data.group_id || packet_data.group_id == "") {
                                packet_data.group_id = addtnl_details.group_id;
                            }
                            if(!packet_data.company_id || packet_data.company_id == "") {
                                packet_data.company_id = addtnl_details.company_id;
                            }
                            if(!packet_data.department_id || packet_data.department_id == "") {
                                packet_data.department_id = addtnl_details.department_id;
                            }
                            if(!packet_data.user_id || packet_data.user_id == "") {
                                packet_data.user_id = addtnl_details.user_id;
                            }
                            if (addtnl_details.driver_id) {
                                packet_data.driver_id = addtnl_details.driver_id;
                            } else {
                                packet_data.driver_id = null;
                            }
                            if (addtnl_details.driver_name) {
                                packet_data.driver_name = addtnl_details.driver_name;
                            } else {
                                packet_data.driver_name = null;
                            }
                            packet_data =  parseAlphadevice(packet_data,addtnl_details);
                            packet_data.vehicle_details = addtnl_details;
                            packet_data.vehicle_avail_status = 1;
                        } else {
                            packet_data.vehicle_avail_status = 0;
                            // console.log('Didnt get  data from elastic search for ' + packet_data.deviceid);
                        }
                        
                        if(packet_data.lat != null && packet_data.long != null && packet_data.lat != '' && packet_data.long != '' && packet_data.lat != undefined && packet_data.long != undefined) {
                            let directquery = {
                                "_source": ["address"],
                                query: {
                                    bool: {
                                        must: [
                                            { match: { lat: parseFloat(packet_data.lat) } },
                                            { match: { lng: parseFloat(packet_data.long) } }
                                        ]
                                    }
                                },
                                size: 1
                            };
                            await getDatafromelasticsearch('geocode_data', 'geocode_data', directquery, 1).then((address) => {
                                // console.log('------------Getting Address data from Elastic Search direct query----------', address.hits.total);
                                if (address != null && address != undefined && address != '' && address.hits.hits.length > 0) {
                                    packet_data.location_name = address.hits.hits[0]._source.address;
                                }
                            }).catch((err) => {
                                console.log('error occured', err);
                            });
                            if (packet_data.location_name == '') {
                                let query = {
                                               "_source": ["address"],
                                                query: {
                                                    bool : {
                                                        must : {
                                                            match_all : {}
                                                        },
                                                        filter : {
                                                            geo_distance : {
                                                                distance : "500m",
                                                                location : {
                                                                    lat : parseFloat(packet_data.lat),
                                                                    lon : parseFloat(packet_data.long)
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            };
                                await getDatafromelasticsearch('geocode_data','geocode_data',query,1).then((address) => {
                                   // console.log('------------Getting Address data from Elastic Search----------',address.hits.total);
                                    if(address != null && address != undefined && address != '' && address.hits.hits.length > 0){
                                        packet_data.location_name = address.hits.hits[0]._source.address;
                                    }
                                }).catch((err) => {
                                    console.log('error occured',err);
                                });
                            }
                            if(packet_data.location_name == '') {
                                await geocodeLocation(parseFloat(packet_data.lat),parseFloat(packet_data.long)).then((address) => {
                                    packet_data.location_name = address;
                                   // console.log('----gort response-----');
                                }).catch((err) => {
                                    console.log('error occured',err);
                                });
                            }
                        }
                        // console.log('you data after processs--');
                        // console.log(packet_data);
                        // await putRecordKinesis(packet_data, process.env.NOTIFICATION_STREAM);
                        // out_packet_data = packet_data;
                        // output_temp_data.recordId = record.recordId;
                        // output_temp_data.result = 'Ok';
                        // packet_data = JSON.stringify(out_packet_data);
                        // output_temp_data.data = (new Buffer(packet_data, 'utf8')).toString('base64');
                        // output_data.push(output_temp_data);
                        let fuel_validity =1;
                        let timeZone = await getDeviceTimezone(packet_data.company_id);
                        packet_data.timeZone = timeZone; 
                        packet_data.device_voltage_type = addtnl_details.device_voltage_type; 
                        if(packet_data.bit_status  &&  (addtnl_details.device_voltage_type == '12' || addtnl_details.device_voltage_type == '24')) {
                            if(addtnl_details.calibration_type == 'calibrated') {
                                packet_data.analog_value = parseInt(packet_data.bit_status.slice(-3));
                                let calibrated_values = addtnl_details.calibrated_values;
                                if(calibrated_values && calibrated_values!=''  &&  calibrated_values!=null) {
                                   // calibrated_values =JSON.parse(calibrated_values);
                                    if(calibrated_values['10'] && calibrated_values['25'] && calibrated_values['50'] && calibrated_values['75'] && calibrated_values['100']) {
                                        let max_analog_value_type  = 255;
                                        if(addtnl_details.device_voltage_type == '12') {
                                            max_analog_value_type = 128; 
                                        }
                                        calibrated_values['0']   = parseFloat(calibrated_values['0'])*max_analog_value_type/100;
                                        calibrated_values['10']  = parseFloat(calibrated_values['10'])*max_analog_value_type/100;
                                        calibrated_values['25']  = parseFloat(calibrated_values['25'])*max_analog_value_type/100;
                                        calibrated_values['50']  = parseFloat(calibrated_values['50'])*max_analog_value_type/100;
                                        calibrated_values['75']  = parseFloat(calibrated_values['75'])*max_analog_value_type/100;
                                        calibrated_values['100'] = parseFloat(calibrated_values['100'])*max_analog_value_type/100;
                                        let max_analog_value = calibrated_values['100'];
                                        let min_analog_value = calibrated_values['0'];
                                        if(packet_data.analog_value >= max_analog_value) {
                                            // packet_data.analog_value = max_analog_value;
                                            // packet_data.fuel_percent = 100;
                                            // packet_data.fuel_litre   = addtnl_details.vehicle_fuel_tank1_capacity;
                                            fuel_validity = 0;
                                            packet_data.fuel_type    = 'calibrated';
                                        } else if(packet_data.analog_value <= min_analog_value) {
                                            // packet_data.analog_value = min_analog_value;
                                            // packet_data.fuel_percent = 0;
                                            // packet_data.fuel_litre   = 0;
                                            fuel_validity = 0;
                                            packet_data.fuel_type    = 'calibrated';
                                        } else if(packet_data.analog_value == 1) {
                                            // packet_data.analog_value = min_analog_value;
                                            // packet_data.fuel_percent = 0;
                                            // packet_data.fuel_litre   = 0;
                                            fuel_validity = 0;
                                            packet_data.fuel_type    = 'calibrated';
                                        } else if(packet_data.analog_value == 2) {
                                            // packet_data.analog_value = min_analog_value;
                                            // packet_data.fuel_percent = 0;
                                            // packet_data.fuel_litre   = 0;
                                            fuel_validity = 0;
                                            packet_data.fuel_type    = 'calibrated';
                                        } else if(packet_data.analog_value == 3) {
                                            // packet_data.analog_value = min_analog_value;
                                            // packet_data.fuel_percent = 0;
                                            // packet_data.fuel_litre   = 0;
                                            fuel_validity = 0;
                                            packet_data.fuel_type    = 'calibrated';
                                        } else if(packet_data.analog_value == 4) {
                                            // packet_data.analog_value = min_analog_value;
                                            // packet_data.fuel_percent = 0;
                                            // packet_data.fuel_litre   = 0;
                                            fuel_validity = 0;
                                            packet_data.fuel_type    = 'calibrated';
                                        } else {
                                            let percent_slab=0,percent_next_slab=0,percent_value=0,percent_next_value=0;
                                            if(packet_data.analog_value > calibrated_values['0'] &&  packet_data.analog_value <= calibrated_values['10']) {
                                                percent_slab        = 0;
                                                percent_next_slab   = 10;
                                                percent_value       = calibrated_values['0'];
                                                percent_next_value  = calibrated_values['10']
                                            } else  if(packet_data.analog_value > calibrated_values['10'] &&  packet_data.analog_value <= calibrated_values['25']) {
                                                percent_slab        = 10;
                                                percent_next_slab   = 25;
                                                percent_value       = calibrated_values['10'];
                                                percent_next_value  = calibrated_values['25']
                                            }  else  if(packet_data.analog_value > calibrated_values['25'] &&  packet_data.analog_value <= calibrated_values['50']) {
                                                percent_slab        = 25;
                                                percent_next_slab   = 50;
                                                percent_value       = calibrated_values['25'];
                                                percent_next_value  = calibrated_values['50']
                                            }  else  if(packet_data.analog_value > calibrated_values['50'] &&  packet_data.analog_value <= calibrated_values['75']) {
                                                percent_slab        = 50;
                                                percent_next_slab   = 75;
                                                percent_value       = calibrated_values['50'];
                                                percent_next_value  = calibrated_values['75']
                                            }  else  if(packet_data.analog_value > calibrated_values['75'] &&  packet_data.analog_value <= calibrated_values['100']) {
                                                percent_slab        = 75;
                                                percent_next_slab   = 100;
                                                percent_value       = calibrated_values['75'];
                                                percent_next_value  = calibrated_values['100'];
                                            } 
                                            let unit_analog_val      = (percent_next_slab-percent_slab)/(percent_next_value - percent_value);
                                            let fuel_percent         =  percent_slab + ((packet_data.analog_value - percent_value)*unit_analog_val);
                                            
                                            // packet_data.percent_slab = percent_slab;
                                            // packet_data.percent_next_slab = percent_next_slab;
                                            // packet_data.percent_value = percent_value;
                                            // packet_data.percent_next_value = percent_next_value;

                                            packet_data.fuel_percent = parseFloat(fuel_percent).toFixed(2);
                                            packet_data.fuel_litre   = parseFloat(((addtnl_details.vehicle_fuel_tank1_capacity*fuel_percent)/100)).toFixed(2);
                                            packet_data.fuel_type    = 'calibrated';
                                        }
                                        // console.log('fuel_processing completed');
                                    }
                                }
                            }
                            else {
                                let max_analog_value  = 255;
                                if(addtnl_details.device_voltage_type == '12') {
                                    max_analog_value = 128; 
                                }
                                packet_data.analog_value = parseInt(packet_data.bit_status.slice(-3));
                                if(packet_data.analog_value >= max_analog_value) {
                                    // packet_data.analog_value = max_analog_value;
                                    fuel_validity = 0;
                                } else if(packet_data.analog_value <=0 ) {
                                    fuel_validity = 0;
                                } else {
                                    packet_data.fuel_percent = parseFloat(((packet_data.analog_value * 100)/max_analog_value)).toFixed(2);
                                    packet_data.fuel_type    = 'calibrating';
                                }
                            }
                            // if((addtnl_details.calibration_type == 'calibrating' || addtnl_details.calibration_type == 'calibrated')  && (addtnl_details.device_voltage_type == '12' || addtnl_details.device_voltage_type == '24')) {
                            //     var max_analog_value = 255;
                            //     packet_data.analog_value = parseInt(packet_data.bit_status.slice(-3));
                                
                            //     if(packet_data.analog_value > max_analog_value) {
                            //         packet_data.analog_value = max_analog_value;
                            //     }
                            //     packet_data.fuel_percent = (packet_data.analog_value * 100)/max_analog_value;
                            //     if(addtnl_details.vehicle_fuel_tank1_capacity && addtnl_details.vehicle_fuel_tank1_capacity > 0) {
                            //         packet_data.fuel_litre   = (addtnl_details.vehicle_fuel_tank1_capacity * packet_data.analog_value)/max_analog_value;
                            //     }
                            // }
                        }
                        if(packet_data.date_formatted != '' &&  packet_data.date_formatted != undefined && packet_data.date_formatted != null){
                            var date_formated_formate = moment(packet_data.date_formatted).format('YYYYMMDD');
                            packet_data.data_day      = date_formated_formate;
                            
                            if(timeZone != 0 && timeZone != null && timeZone != undefined){
                                packet_data.date_formatted = moment(packet_data.date_formatted).utcOffset(timeZone).format('YYYY-MM-DDTHH:mm:ss.SSSSZ');
                            } else {
                                packet_data.date_formatted = moment.utc(packet_data.date_formatted).format('YYYY-MM-DDTHH:mm:ss.SSSSZ');
                            }
                        } else {
                            packet_data.data_day = '';   
                        }   
                        packet_data.valid_data = 0;
                        let data_validity = 1;
                        if(packet_data.vehicle_status == "0" && parseFloat(packet_data.speed) > 5) {
                            data_validity  = 0;
                        } else if((parseFloat(packet_data.speed) - parseFloat(packet_data.sog)) > 45) {
                            data_validity  = 0;
                        }                        
                        // bulkSecondaryVDetailsInsert.push({
                        //     index: {
                        //         _index: 'vehicle_details',
                        //         _type: 'vehicle_details'
                        //     }
                        // });
                        // bulkSecondaryVDetailsInsert.push(packet_data);
                        let domain_name         = 'default';
                        if(packet_data.group_id && packet_data.group_id != '' && packet_data.group_id != null && packet_data.group_id != undefined) {
                            if(data_validity == 1){
                                if(packet_data.lat  && packet_data.lat!="" && packet_data.long && packet_data.long!="" && packet_data.lat != null && packet_data.long != null && packet_data.date_formatted && moment(packet_data.date_formatted).isValid() &&  (packet_data.status=='A' || packet_data.status=='H' || packet_data.status=='L')) {
                                    let days_diff = Math.abs(moment().diff(packet_data.date_formatted, 'days'));
                                    if(days_diff  <= 2) {
                                        packet_data.valid_data = 1;
                                        let last_date  = null;
                                        if(unicDetails[packet_data.deviceid]) {
                                            last_date = unicDetails[packet_data.deviceid];
                                        } else {
                                            let unic_last_query = {
                                                "_source": ["date"],
                                                query: {
                                                    bool: {
                                                        "must": [
                                                            {
                                                              "term": {
                                                                "deviceid": packet_data.deviceid
                                                              }
                                                            }
                                                        ]
                                                    }
                                                },
                                                size: 1,
                                                sort: {
                                                  "date": {
                                                    "order": "desc"
                                                  }
                                                }
                                            };
                                            await getDatafromelasticsearch('unic_vehicle_status', 'unic_vehicle_status', unic_last_query, 1).then((unic_details) => {
                                                // console.log('------------Getting last data from unic----------', unic_details.hits.total);
                                                if (unic_details != null && unic_details != undefined && unic_details != '' && unic_details.hits.hits.length > 0  &&  unic_details.hits.hits[0]._source.date) {
                                                    last_date = unic_details.hits.hits[0]._source.date;
                                                    unicDetails[packet_data.deviceid] = last_date;
                                                }
                                            }).catch((err) => {
                                            });
                                        }
                                        packet_data.last_date = last_date;
                                        if(last_date == null || last_date < packet_data.date) {
                                            unicDetails[packet_data.deviceid] = packet_data.date;
                                            unicParams.push({
                                                index: {
                                                    _index: 'unic_vehicle_status',
                                                    _type: 'unic_vehicle_status',
                                                    _id: packet_data.deviceid
                                                }
                                            });
                                            unicParams.push(packet_data);
                                        }
                                        // packet_data.valid_data = 1;
                                        // unicParams.push({
                                        //     index: {
                                        //         _index: 'unic_vehicle_status',
                                        //         _type: 'unic_vehicle_status',
                                        //         _id: packet_data.deviceid
                                        //     }
                                        // });
                                        // unicParams.push(packet_data);
                                    }
                                }  else{
                                    // Nothing to do
                                }
                            }
                            if(domains[`group_${packet_data.group_id}`]) {
                                //temporary code for data push
                                domain_name = `group_${packet_data.group_id}`;
                                var tempdata = Object.assign({}, packet_data);
                                tempdata.group_enabled_data = 1;
                                domaindata[domain_name].push({
                                    index: {
                                        _index: 'vehicle_details',
                                        _type: 'vehicle_details'
                                    }
                                });
                                domaindata[domain_name].push(tempdata);
                                //temporary code ends
                            } else {
                                domaindata[domain_name].push({
                                    index: {
                                        _index: 'vehicle_details',
                                        _type: 'vehicle_details'
                                    }
                                });
                                domaindata[domain_name].push(packet_data);
                           }
                        } else {
                            unicParams.push({
                                index: {
                                    _index: 'unic_vehicle_status',
                                    _type: 'unic_vehicle_status',
                                    _id: packet_data.deviceid
                                }
                            });
                            unicParams.push(packet_data);
                            domaindata[domain_name].push({
                                index: {
                                    _index: 'vehicle_details',
                                    _type: 'vehicle_details'
                                }
                            });
                            domaindata[domain_name].push(packet_data);
                        }
                        // let domain_name         = domains['default'];
                        // if(packet_data.group_id && packet_data.group_id!='') {
                        //     let group_id         = packet_data.group_id.toString();
                        //     if(domains['group_'+group_id]) {
                        //         //temporary code for data push
                        //         var tempdata = Object.assign({}, packet_data);
                        //         tempdata.group_enabled_data = 1;
                        //         domaindata[domain_name].push({
                        //             index: {
                        //                 _index: 'vehicle_details',
                        //                 _type: 'vehicle_details'
                        //             }
                        //         });
                        //         domaindata[domain_name].push(tempdata);
                        //         //temporary code ends
                                
                        //         domain_name          = domains['group_'+group_id];
                        //     }
                        // }
                        // domaindata[domain_name].push({
                        //     index: {
                        //         _index: 'vehicle_details',
                        //         _type: 'vehicle_details'
                        //     }
                        // });
                        // domaindata[domain_name].push(packet_data);

                        // unicParams.push({
                        //     index: {
                        //         _index: 'vehicle_details'+index_name,
                        //         _type: 'vehicle_details'
                        //     }
                        // });
                        // unicParams.push(packet_data);
                        // packet_data.data_sync = 1;
                        if(packet_data.valid_data ==1) {
                            await putRecordKinesis(packet_data, process.env.NOTIFICATION_STREAM);
                        }
                        // console.log('your data after processs--',packet_data);
                        // Event data push start
                       // console.log('--started checking for  events----');
                        // await putRecordEventKinesis(packet_data);
                        // if(packet_data) {
                        //     var s3filename = packet_data.deviceid + "_" + (packet_data.date || moment().format())
                        //     await uploadFileOnS3(s3filename,packet_data);
                        // }
                        // Event data push end
                        out_packet_data = packet_data;
                        out_orginal_data = packet_data;
                        output_temp_data.recordId = record.recordId;
                        output_temp_data.result = 'Ok';
                        packet_data = JSON.stringify(out_packet_data);
                        output_temp_data.data = (new Buffer.from(packet_data, 'utf8')).toString('base64');
                        output_data.push(output_temp_data);
                        callback2();
                    }
                    Iterator();
                }, function pushbackToFirebase(err) {
                    if (err) {
                        console.log('error in processing',err);
                        console.log(err);
                        resolve();
                    }
                    console.log(`-----------Processing completed.  Successful records -------------- ${output_data.length}.`);
                    resolve();
                });
            } else {
                resolve();
            } 
        });
    }
    function upsertElasticSearch(params,client_variable) {
        return new Promise((resolve, reject) => {
            client_variable.bulk({
                body: params,
                requestTimeout:100000
            }, function (err, resp) {
                if (err) {
                    console.log("Bulk Upsert to elastic search failed--",params.length);
                    resolve('error');
                } else {
                    if(resp.errors == true) {
                        console.log('---getting error in upserting latest data---',params.length);
                    } else {
                        console.log("Successfully Upsert to elastic search----",params.length);
                    }
                    resolve(resp);
                }
                });
        });
    }
    function pushDatatoelasticsearch(data,indexname,typename) {
        return new Promise(resolve => {
            async function asyncdatapush() {
                await connecttoElasticsearch();
                if(elasticsearchconnectionflag == 0) {
                    resolve();
                } else {
                    var flag = 0;
                    elastic_search_client.index({
                        index: indexname,
                        type: typename,
                        body: data
                    }, function (err, resp, status) {
                        flag = 1;
                        resolve();
                    });
                    setTimeout(function(){
                        if(flag == 0) {
                            resolve();
                        }
                    },5000);
                }
            }
            asyncdatapush();
        });
    }
    function getDatafromelasticsearch(indexname,typename,params,limit) {
        return new Promise((resolve, reject) => {
            async function getgeodata() {
                await connecttoElasticsearch();
                elastic_search_client.search({
                    index: indexname,
                    type: typename,
                    body: params,
                    size:limit
                }, (error, response) => {
                    if (error)
                        reject(error)
                    else
                        resolve(response)
                })
            }
            getgeodata();
        });
    }
    function geocodeLocation(lat,lng) {
        return new Promise(resolve => {
            if(!lat || !lng || lng =='' || lat== '') {
                return resolve('');
            }
          //  console.log('---fetching geo details----');
            // console.log(lat);
            // console.log(lng);
            unirest.get('https://maps.googleapis.com/maps/api/geocode/json?latlng='+lat+','+lng+'&key='+process.env.GOOGLE_API_KEY)
            .timeout(10000)
            .headers({'Accept': 'application/json'})
            .end(function (response) {
               // console.log('got response from google map');
               // console.log(response.body);
                if(response && response.body && response.body.results && response.body.results[0]) {
                    let res =response.body.results;
                    async function parsegeocodeData() {
                        let data = {
                            lat:lat,
                            lng:lng,
                            "location": {
                                "lat" : lat,
                                "lon":lng
                            },
                            address:res[0].formatted_address
                        };
                       //  console.log(data);
                        await pushDatatoelasticsearch(data,'geocode_data','geocode_data').then((address) => {
                            resolve(res[0].formatted_address);
                        }).catch((err) => {
                         //   console.log('error occured',err);
                            resolve('');
                        });
                    }
                    parsegeocodeData();
                } else {
                    resolve('');
                }
            });
            // geocoder.reverse({lat:lat, lon:lng}, function(err, res) {
            //     console.log('output');
            //     console.log(err);
            //     if(res && res[0]) {
            //         async function parsegeocodeData() {
            //             let data = {
            //                 lat:lat,
            //                 lng:lng,
            //                 "location": {
            //                     "lat" : lat,
            //                     "lon":lng
            //                 },
            //                 address:res[0].formattedAddress
            //             };
            //             await pushDatatoelasticsearch(data,'geocode_data','geocode_data').then((address) => {
            //                 resolve(res[0].formattedAddress);
            //             }).catch((err) => {
            //                 console.log('error occured',err);
            //             });
            //         }
            //         parsegeocodeData();
            //     } else {
            //         resolve('');
            //     }
            // });
        });
    }
    function putRecordKinesis(packet_data, streamname) {
        return new Promise(resolve => {
            console.log('started pushing to kinesis');
            kinesis.putRecords({
                Records: [{
                    Data: new Buffer.from(JSON.stringify({
                        packet: packet_data
                    })),
                    PartitionKey: String(new Date())
                }], StreamName: streamname
            }, function (err, data) {
                if (err) {
                    console.log("Kinesis Error", err);
                    resolve('error');
                } else {
                    console.log("Successfully pushed to Kinesis----"+streamname);
                    resolve();
                }

            });
        });
    } 
    function uploadFileOnS3(fileName, fileData) {
        var s3bufferObject = new Buffer.from(JSON.stringify(fileData));
        return new Promise((resolve, reject) => {
            var params = { 
                Key: fileName,
                Body: s3bufferObject,
                ContentType: "application/json"
            };
            // console.log('S3 Data Object',params);
            s3bucket.upload(params, function (err, res) {
                if (err) {
                    // console.log("Error in uploading backup file on s3 due to " + err);
                    resolve(err);
                } else {
                    // console.log("Backup File successfully uploaded To S3.");
                    resolve(res);
                }
            });
        });
    }

    function insertToSecondary(bulkDataArray = [], elastic_client) {
        return elastic_client.bulk({ body: bulkDataArray });
    }

    var output_data = []
    await redisConnection();
    await connecttoElasticsearch('default');
    await asyncRecordGet();

    console.log('started inserting domain data--');
    for (let dmdata_key in domaindata) { // iterate Against each keys: bulk array
        let domain_bulk_array = domaindata[dmdata_key]; // moving bulk insert array to variable
        console.log('connecting domains----', dmdata_key); // print group_ID
        console.log('domain data length--', domain_bulk_array.length);
        if (domain_bulk_array.length > 0) { // if array exsist with length 
            await connecttoElasticsearch(dmdata_key); // establish new connection
            while (domain_bulk_array.length) {
                let unciarray = domain_bulk_array.splice(0, 10000);
                console.log('upserting to elasticsearch domain---', domain_bulk_array.length);
                await upsertElasticSearch(unciarray, connections[dmdata_key]);
            }
        }
        console.log('completed iteration', dmdata_key);
        if(dmdata_key == 'default' && unicParams.length >0 ){
            while (unicParams.length) {
                console.log('upsertin to elasticsearch to unic default---', unicParams.length);
                let unciarray = unicParams.splice(0, 10000);
                await upsertElasticSearch(unciarray, connections['default']);
            }
        }
    }

    if(out_orginal_data && out_orginal_data.length > 0) {
        let random_number = Math.floor(Math.random() * Math.floor(10000));
        var s3filename = "Alpha_" + random_number +'_' + Date.now();
        await uploadFileOnS3(s3filename,out_orginal_data);
    }
    console.log('---------------OUTPUT DATA---------');
    console.log(`Elastic search write completed Successful records after domain ${output_data.length}.`);
    return { statusCode: 200,records: output_data };
};