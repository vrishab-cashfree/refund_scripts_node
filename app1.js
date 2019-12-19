const util = require('util');
const fs = require('fs');
const request = require('request');

const sleep = util.promisify(setTimeout);
const readFileAsync = util.promisify(fs.readFile);
const appendFileAsync = util.promisify(fs.appendFile);
const postAsync = util.promisify(request.post);



const config = require('./config.json');
const {appId, secretKey, env, file} = config; 
const baseUrl = config["url"][env];
const url = baseUrl + "order/info/status";
const headers = {'Content-Type': 'application/x-www-form-urlencoded'}
const writeFile = __dirname + "/refunds.csv";

//read csv file
const getData = async function(fileName){
    try{
        return await readFileAsync(fileName, "utf8");
    }
    catch(err){
        console.log("getData::err:", err);
    }
};

//write into refund file file
const writeData = async function(refund){
    try{
        const {referenceId, refundAmount, refundNote} = refund
        const refundRow = referenceId + "," + refundAmount + "," + refundNote + "\n";
        await appendFileAsync(writeFile, refundRow);
    }
    catch(err){
        console.log("writeData::err:", err);
    }
}

//format data into chunks of 30 refunds
const formatData = function (data){
    //setting chunksize as 30 due to rate limiting on cashfree's end
    const chunkSize = 30;
    let refundSetArray = [];
    let localLength = 0;
    let refundSetIndex = 0;
    refundSetArray[0] = [];

    const refundRows = data.split(/\r?\n/);
    console.log(refundRows.length);
    refundRows.forEach(refundRow => {
        [orderId, refundAmount, refundNote] = refundRow.split(',')
        const refund = {orderId, refundAmount, refundNote};

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




const getTransactionId = async function(transaction){
    try{
        const {orderId} = transaction;
        const form = {appId, secretKey, orderId};
        const r = await postAsync({url,headers, form, family: 4,});
        const {status, referenceId, reason} = JSON.parse(r.body);
        if(status === 'ERROR') throw {name: 'getTransactionError', message: 'err returned by server:', reason}
        const writeDataObj = {...transaction};
        writeDataObj.referenceId = referenceId;
        writeData(writeDataObj);
    }
    catch(err){
        console.log('err in getting transaction id');
        console.log(err);
    }
};

const getTransactions = async function(transactions){
    return transactions.map(transaction => {
        getTransactionId(transaction);
    });
}

const getTransactionChunks = async function(transactionChunks){
    for(let i = 0; i< transactionChunks.length; i++){
        getTransactions(transactionChunks[i]);
        await sleep(60 * 1000);
    }
};

(async () => {
    try{
        const rawRefundData = await getData(file);
        const refundChunks = formatData(rawRefundData);
        console.log(refundChunks);
        await getTransactionChunks(refundChunks);
    }
    catch(err){
        console.log("err in main execution flow");
        console.log(err);
    }

})();
