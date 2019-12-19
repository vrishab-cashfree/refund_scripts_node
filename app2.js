const util = require('util');
const fs = require('fs');
const request = require('request');

const sleep = util.promisify(setTimeout);
const readFileAsync = util.promisify(fs.readFile);
const appendFileAsync = util.promisify(fs.appendFile);
const postAsync = util.promisify(request.post);

//read from config
const config = require('./config.json');
const {appId, secretKey, env} = config; 
const baseUrl = config["url"][env];
const url = baseUrl + 'order/refund';
const headers = {'Content-Type': 'application/x-www-form-urlencoded'}
const file = __dirname + "/refunds.csv";
const errorFile = __dirname + "/error.csv";

//format data into chunks of 30 refunds
const formatData = function (data){
    //setting chunksize as 30 due to rate limiting on cashfree's end
    const chunkSize = 30;
    let refundSetArray = [];
    let localLength = 0;
    let refundSetIndex = 0;
    refundSetArray[0] = [];

    const refundRows = data.split(/\r?\n/);
    refundRows.forEach(refundRow => {
        [referenceId, refundAmount, refundNote] = refundRow.split(',')
        const refund = {referenceId, refundAmount, refundNote};

        if(localLength===chunkSize){
            refundSetIndex = refundSetIndex + 1;
            refundSetArray[refundSetIndex] = [];
            localLength = 0;
        }
        
        localLength = localLength + 1;
        refundSetArray[refundSetIndex].push(refund);
    });
    return refundSetArray;
};
//read csv file
const getData = async function(fileName){
    try{
        return await readFileAsync(fileName, "utf8");
    }
    catch(err){
        console.log("getData::err:", err);
    }
};

//write into error file
const writeData = async function(refund){
    try{
        const {referenceId, refundAmount, refundNote, err} = refund
        const refundRow = referenceId + "," + refundAmount + "," + refundNote + "," + err + "\n";
        await appendFileAsync(errorFile, refundRow);
    }
    catch(err){
        console.log("writeData::err:", err);
    }
};

const makeRefundCall = async function(refund){
    try{
        const form = {appId,secretKey, ...refund};
        const r = await postAsync({url,headers, form, family: 4,});
        const {status, message, reason} = JSON.parse(r.body); 
        if(status === 'ERROR') throw {name: 'refundError', message: 'err returned by server:', reason}
    }
    catch(err){
        console.log("makeRefundCall::err:", err);
        writeData({err: err.reason, ...refund});
    }
};

const callRefunds = function(refunds){
    return refunds.map((refund) => {
        makeRefundCall(refund);
    });
};

const processRefundChunks = async function(refundChunks){
    try{
        for(let i = 0; i< refundChunks.length; i++){
            callRefunds(refundChunks[i]);
            await sleep(60 * 1000);
        }
    }
    catch(err){
        console.log("processRefundChunks::err:", err);
    }
};


(async () => {
    try{
        const rawRefundData = await getData(file);
        const refundChunks = formatData(rawRefundData);
        console.log(refundChunks);
        processRefundChunks(refundChunks);
    }
    catch(err){
        console.log("err in main execution flow");
        console.log(err);
    }
})();
